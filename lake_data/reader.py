"""按指定快照还原可见行的读取层。

口径全部以本项目 docs/data-contract.md 与快照清单为准，只实现这套冻结的
JSONL 湖表子集，不冒充兼容某个湖表产品的完整格式。

每次读取：
  1. 冻结快照与引用文件的全部身份摘要（字节数、sha256、序列与 schema/spec）；
  2. 按字段 ID 把各数据文件投影到快照读 schema；
  3. 为 position 删除建 (uri, position) 索引、为 equality 删除按键集合建
     哈希索引，逐行只做字典查找，不重复扫描删除文件；
  4. 输出可见行、原文件物理位置、schema 投影与实际命中的删除证据。

删除目标不存在、行号越界或元数据身份不符一律抛 LakeDataError，
不会被静默当成“读取完整”。
"""
from dataclasses import dataclass, field

from .errors import LakeDataError
from .io import asset, integer, load
from .jsonl_io import decode_rows, parse_jsonl_rows
from .partition import PartitionSpec, build_spec, check_file_partition, transform_row
from .schema import (
    build_projection,
    build_schema,
    decode_field_value,
    eq_tuple,
    project_row,
    to_output_value,
)


def _asset(root, ref):
    """资产读取；把 io 层的身份/越界错误统一成 LakeDataError。"""
    try:
        return asset(root, ref)
    except LakeDataError:
        raise
    except ValueError as exc:
        raise LakeDataError(f"资产 {ref.get('path')} 身份不符或不可读: {exc}") from None


# ---------------------------------------------------------------- 内部结构

@dataclass
class _DataFile:
    manifest: dict
    file_schema: object
    digest: str
    rows: list                      # [(position, {fid: 解码值})] 文件 schema 口径
    projected: list                 # [(position, {fid: 读 schema 口径值})]
    plan: tuple
    type_lookup: dict               # 分区变换用的字段类型表
    partition_values: list          # 每行按文件自身 spec 求出的分区 tuple


@dataclass
class _PositionDelete:
    manifest: dict
    spec: PartitionSpec
    entries: list                   # [{"uri","position","record"}]


@dataclass
class _EqualityDelete:
    manifest: dict
    spec: PartitionSpec
    field_ids: tuple
    key_plan: tuple                 # 删除 schema -> 读 schema 的投影计划
    keys: list                      # [(eq_tuple, 原始可输出值, record_index)]


@dataclass
class _Index:
    # (uri, position) -> [证据 dict]
    position: dict = field(default_factory=dict)
    # field_ids tuple -> {eq_tuple: [证据 dict]}
    equality: dict = field(default_factory=dict)
    distinct_equality_keys: int = 0


# ---------------------------------------------------------------- 清单校验

def _check_unique_ids(items, what):
    seen = set()
    for item in items:
        fid = item['id']
        if type(fid) is not int or fid in seen:
            raise LakeDataError(f'{what} id 缺失或重复: {fid!r}')
        seen.add(fid)


def _check_unique_file_ids(items, what):
    seen = set()
    for item in items:
        fid = item['id']
        if not isinstance(fid, str) or not fid or fid in seen:
            raise LakeDataError(f'{what} id 缺失或重复: {fid!r}')
        seen.add(fid)


def _validate_file_identity(entry):
    integer(entry['data_sequence'])
    integer(entry['file_sequence'])
    if not isinstance(entry.get('partition'), list):
        raise LakeDataError(f'文件 {entry["id"]} 分区 tuple 非法')


def _validate_data_identity(entry):
    _validate_file_identity(entry)
    if not isinstance(entry.get('uri'), str) or not entry['uri']:
        raise LakeDataError(f'数据文件 {entry.get("id")} 缺少规范 URI')


def _load_manifest(manifest_path):
    v, digest = load(manifest_path)
    for key in ('schemas', 'partition_specs', 'files', 'snapshot'):
        if key not in v:
            raise LakeDataError(f'清单缺少顶层段落: {key}')
    _check_unique_ids(v['schemas'], 'schema')
    _check_unique_ids(v['partition_specs'], 'partition_spec')
    _check_unique_file_ids(v['files'], 'file')

    schemas = {s['id']: build_schema(s) for s in v['schemas']}
    specs = {s['id']: build_spec(s, schemas) for s in v['partition_specs']}

    snap = v['snapshot']
    if snap['schema_id'] not in schemas:
        raise LakeDataError(f"快照读 schema 未知: {snap['schema_id']}")
    file_ids = snap['file_ids']
    if not isinstance(file_ids, list) or not file_ids:
        raise LakeDataError('快照 file_ids 必须是非空列表')
    if len(set(file_ids)) != len(file_ids):
        raise LakeDataError('快照 file_ids 存在重复引用')

    by_id = {f['id']: f for f in v['files']}
    missing = [fid for fid in file_ids if fid not in by_id]
    if missing:
        raise LakeDataError(f'快照引用了清单中不存在的文件: {missing}')
    return v, digest, schemas, specs, by_id, snap


# ---------------------------------------------------------------- 数据文件

def _build_data_file(entry, root, schemas, specs, read_schema):
    _validate_data_identity(entry)
    if entry['schema_id'] not in schemas:
        raise LakeDataError(f"数据文件 {entry['id']} schema 未知: {entry['schema_id']}")
    if entry['spec_id'] not in specs:
        raise LakeDataError(f"数据文件 {entry['id']} spec 未知: {entry['spec_id']}")
    file_schema = schemas[entry['schema_id']]
    spec = specs[entry['spec_id']]
    if len(entry['partition']) != len(spec.fields):
        raise LakeDataError(f"数据文件 {entry['id']} 分区宽度与 spec {spec.id} 不符")

    raw, digest = _asset(root, entry['asset'])
    parsed = decode_rows(raw)
    if len(parsed) != integer(entry['asset'].get('rows', -1)):
        raise LakeDataError(f"数据文件 {entry['id']} 物理行数与资产摘要不符")

    _, plan = build_projection(file_schema, read_schema)
    type_lookup = {**read_schema.by_id, **file_schema.by_id}

    decoded_rows, projected_rows, partition_values = [], [], []
    for position, row in parsed:
        # 1) 文件自身 schema 校验与解码
        for fid in row:
            if fid not in file_schema.by_id:
                raise LakeDataError(
                    f"数据文件 {entry['id']} 物理行 {position} 含 schema 未知字段 {fid}")
        decoded = {}
        for fid, value in row.items():
            decoded[fid] = decode_field_value(value, file_schema.by_id[fid].type, fid)
        for f in file_schema.fields:
            if f.required:
                if f.id not in decoded:
                    raise LakeDataError(
                        f"数据文件 {entry['id']} 物理行 {position} 缺必选字段 {f.id}")
                if decoded[f.id] is None:
                    raise LakeDataError(
                        f"数据文件 {entry['id']} 物理行 {position} 必选字段 {f.id} 为 null")
        # 2) 投影到读 schema（含旧文件缺列补 null 与类型演化检查）
        projected = project_row(row, plan, file_schema)
        # 3) 按文件自身 spec 求分区，并与声明分区核对
        partition_values.append(transform_row(decoded, spec, type_lookup))
        decoded_rows.append((position, decoded))
        projected_rows.append((position, projected))

    check_file_partition(partition_values, entry['partition'], spec)
    return _DataFile(entry, file_schema, digest, decoded_rows, projected_rows,
                     plan, type_lookup, partition_values)


# ---------------------------------------------------------------- 删除文件

def _build_position_delete(entry, specs, data_by_uri):
    _validate_file_identity(entry)
    spec = specs.get(entry['spec_id'])
    if spec is None:
        raise LakeDataError(f"position 删除 {entry['id']} spec 未知")
    if len(entry['partition']) != len(spec.fields):
        raise LakeDataError(f"position 删除 {entry['id']} 分区宽度与 spec 不符")
    parsed = parse_jsonl_rows(entry['_raw'])
    if len(parsed) != integer(entry['asset'].get('rows', -1)):
        raise LakeDataError(f"position 删除 {entry['id']} 行数与资产摘要不符")
    entries = []
    for record, (_, row) in enumerate(parsed):
        if set(row) != {'file', 'position'}:
            raise LakeDataError(
                f"position 删除 {entry['id']} 记录 {record} 必须只含 file/position")
        uri = row['file']
        pos = row['position']
        if not isinstance(uri, str) or not uri:
            raise LakeDataError(f"position 删除 {entry['id']} 记录 {record} URI 非法")
        if type(pos) is not int or pos < 0:
            raise LakeDataError(f"position 删除 {entry['id']} 记录 {record} 行号非法")
        target = data_by_uri.get(uri)
        if target is None:
            raise LakeDataError(
                f"position 删除 {entry['id']} 目标 URI 不在快照引用数据文件中: {uri}")
        if pos >= len(target.rows):
            raise LakeDataError(
                f"position 删除 {entry['id']} 行号 {pos} 越界（{uri} 共 {len(target.rows)} 行）")
        entries.append({'uri': uri, 'position': pos, 'record': record})
    return _PositionDelete(entry, spec, entries)


def _build_equality_delete(entry, root, schemas, specs, read_schema):
    _validate_file_identity(entry)
    spec = specs.get(entry['spec_id'])
    if spec is None:
        raise LakeDataError(f"equality 删除 {entry['id']} spec 未知")
    if len(entry['partition']) != len(spec.fields):
        raise LakeDataError(f"equality 删除 {entry['id']} 分区宽度与 spec 不符")
    del_schema = schemas.get(entry['schema_id'])
    if del_schema is None:
        raise LakeDataError(f"equality 删除 {entry['id']} schema 未知")

    field_ids = entry.get('field_ids')
    if not isinstance(field_ids, list) or not field_ids:
        raise LakeDataError(f"equality 删除 {entry['id']} field_ids 非法")
    if len(set(field_ids)) != len(field_ids):
        raise LakeDataError(f"equality 删除 {entry['id']} field_ids 重复")
    field_tuple = tuple(field_ids)
    for fid in field_tuple:
        if fid not in del_schema.by_id:
            raise LakeDataError(
                f"equality 删除 {entry['id']} 字段 {fid} 在其 schema 中不可识别")
        if fid not in read_schema.by_id:
            raise LakeDataError(
                f"equality 删除 {entry['id']} 字段 {fid} 在读 schema 中不可识别")

    # 删除键统一投影到读 schema 口径，保证与数据行同口径比较
    _, key_plan = build_projection(del_schema, read_schema)
    key_plan = tuple(step for step in key_plan if step[0] in set(field_tuple))

    raw, _ = _asset(root, entry['asset'])
    parsed = decode_rows(raw)
    if len(parsed) != integer(entry['asset'].get('rows', -1)):
        raise LakeDataError(f"equality 删除 {entry['id']} 行数与资产摘要不符")

    keys = []
    wanted = set(field_tuple)
    for record, (_, row) in enumerate(parsed):
        if set(row) != wanted:
            raise LakeDataError(
                f"equality 删除 {entry['id']} 记录 {record} 键列与 field_ids 不一致")
        for fid, value in row.items():
            # 按删除文件自身 schema 校验键值物理类型
            decode_field_value(value, del_schema.by_id[fid].type, fid)
        projected = project_row(row, key_plan, del_schema)
        key_values = tuple(projected[fid] for fid in field_tuple)
        keys.append((eq_tuple(key_values),
                     [to_output_value(v) for v in key_values], record))
    return _EqualityDelete(entry, spec, field_tuple, key_plan, keys)


# ---------------------------------------------------------------- 索引

def _build_index(position_deletes, equality_deletes, max_keys):
    index = _Index()
    for pd in position_deletes:
        m = pd.manifest
        for e in pd.entries:
            evidence = {
                'kind': 'position',
                'delete_file_id': m['id'],
                'data_sequence': m['data_sequence'],
                'file_sequence': m['file_sequence'],
                'target_uri': e['uri'],
                'target_position': e['position'],
                'record': e['record'],
            }
            index.position.setdefault((e['uri'], e['position']), []).append(
                (m['data_sequence'], pd.spec, tuple(m['partition']), evidence))

    distinct = set()
    for ed in equality_deletes:
        m = ed.manifest
        bucket = index.equality.setdefault(ed.field_ids, {})
        for key, raw_values, record in ed.keys:
            distinct.add((ed.field_ids, key))
            evidence = {
                'kind': 'equality',
                'delete_file_id': m['id'],
                'data_sequence': m['data_sequence'],
                'file_sequence': m['file_sequence'],
                'field_ids': list(ed.field_ids),
                'key': raw_values,
                'record': record,
            }
            bucket.setdefault(key, []).append(
                (m['data_sequence'], ed.spec, tuple(m['partition']), evidence))
    index.distinct_equality_keys = len(distinct)
    if max_keys is not None and len(distinct) > integer(max_keys):
        raise LakeDataError(
            f'相等删除键数 {len(distinct)} 超过清单上限 {max_keys}')
    return index


def _partition_matches(data_row, spec, type_lookup, declared_partition):
    """删除文件的分区限制：用删除文件自己的 spec 作用在候选原行上。"""
    if spec.is_unpartitioned:
        return True  # 未分区删除适用于所有分区
    got = transform_row(data_row, spec, type_lookup)
    return got == declared_partition


# ---------------------------------------------------------------- 主流程

def read_snapshot(manifest_path):
    """按清单快照还原可见行，返回完整读取结果（普通 dict/list 结构）。"""
    from pathlib import Path
    root = Path(manifest_path).parent
    v, manifest_digest, schemas, specs, by_id, snap = _load_manifest(manifest_path)
    read_schema = schemas[snap['schema_id']]

    data_files, position_deletes, equality_deletes = [], [], []
    data_by_uri = {}
    referenced = []
    for fid in snap['file_ids']:
        entry = by_id[fid]
        kind = entry.get('kind')
        if kind == 'data':
            df = _build_data_file(entry, root, schemas, specs, read_schema)
            if entry['uri'] in data_by_uri:
                raise LakeDataError(f"数据文件 URI 重复: {entry['uri']}")
            data_by_uri[entry['uri']] = df
            data_files.append(df)
            referenced.append(entry)
        elif kind == 'position':
            # 先取资产字节，目标校验需要数据文件全部就位，故分两阶段
            raw, digest = _asset(root, entry['asset'])
            entry['_raw'], entry['_digest'] = raw, digest
            referenced.append(entry)
        elif kind == 'equality':
            ed = _build_equality_delete(entry, root, schemas, specs, read_schema)
            equality_deletes.append(ed)
            referenced.append(entry)
        else:
            raise LakeDataError(f'文件 {fid} kind 不受支持: {kind!r}')

    for fid in snap['file_ids']:
        entry = by_id[fid]
        if entry.get('kind') == 'position':
            position_deletes.append(
                _build_position_delete(entry, specs, data_by_uri))

    max_keys = v.get('max_delete_keys')
    index = _build_index(position_deletes, equality_deletes, max_keys)

    visible, deleted = [], []
    for df in data_files:
        m = df.manifest
        for (position, decoded), (_, projected) in zip(df.rows, df.projected):
            hits = []
            # ---- position 删除：data_sequence 不大于删除序列才生效
            for del_seq, spec, part, evidence in index.position.get(
                    (m['uri'], position), ()):
                if m['data_sequence'] <= del_seq and _partition_matches(
                        decoded, spec, df.type_lookup, part):
                    hits.append(evidence)
            # ---- equality 删除：仅对 data_sequence 严格更小的数据文件生效
            for field_tuple, key_bucket in index.equality.items():
                if any(fid not in df.type_lookup for fid in field_tuple):
                    continue  # 该文件早于键列出现，无法形成相同键
                key = eq_tuple(tuple(projected.get(fid) for fid in field_tuple))
                for del_seq, spec, part, evidence in key_bucket.get(key, ()):
                    if m['data_sequence'] < del_seq and _partition_matches(
                            decoded, spec, df.type_lookup, part):
                        hits.append(evidence)

            locator = {
                'file_id': m['id'],
                'uri': m['uri'],
                'position': position,
                'data_sequence': m['data_sequence'],
                'file_sequence': m['file_sequence'],
            }
            row_out = {str(fid): to_output_value(val)
                       for fid, val in projected.items()}
            if hits:
                deleted.append({**locator, 'row': row_out, 'evidence': hits})
            else:
                visible.append({**locator, 'row': row_out})

    return {
        'snapshot': _freeze_block(snap, manifest_digest, referenced),
        'read_schema': _schema_block(read_schema),
        'projections': [_projection_block(df, read_schema) for df in data_files],
        'visible_rows': visible,
        'deleted_rows': deleted,
        'counts': {
            'data_files': len(data_files),
            'position_delete_files': len(position_deletes),
            'equality_delete_files': len(equality_deletes),
            'visible': len(visible),
            'deleted': len(deleted),
            'distinct_equality_keys': index.distinct_equality_keys,
        },
    }


def _freeze_block(snap, manifest_digest, referenced):
    return {
        'snapshot_id': snap['id'],
        'snapshot_schema_id': snap['schema_id'],
        'manifest_sha256': manifest_digest,
        'file_ids': [e['id'] for e in referenced],
        'files': [
            {
                'id': e['id'],
                'kind': e['kind'],
                **({'uri': e['uri']} if e['kind'] == 'data' else {}),
                'schema_id': e['schema_id'],
                'spec_id': e['spec_id'],
                'partition': e['partition'],
                'data_sequence': e['data_sequence'],
                'file_sequence': e['file_sequence'],
                'asset': {
                    'path': e['asset']['path'],
                    'bytes': e['asset']['bytes'],
                    'sha256': e['asset']['sha256'],
                    'rows': e['asset']['rows'],
                },
            }
            for e in referenced
        ],
    }


def _schema_block(read_schema):
    return {
        'schema_id': read_schema.id,
        'fields': [
            {'id': f.id, 'name': f.name, 'type': f.type, 'required': f.required}
            for f in read_schema.fields
        ],
    }


def _projection_block(df, read_schema):
    m = df.manifest
    columns, missing_optional, widened = [], [], []
    for fid, file_type, read_field, do_widen in df.plan:
        if file_type is None:
            missing_optional.append(fid)
            columns.append({
                'field_id': fid,
                'file_name': None,
                'file_type': None,
                'read_name': read_field.name,
                'read_type': read_field.type,
                'widened': False,
            })
        else:
            if do_widen:
                widened.append(fid)
            columns.append({
                'field_id': fid,
                'file_name': df.file_schema.by_id[fid].name,
                'file_type': file_type,
                'read_name': read_field.name,
                'read_type': read_field.type,
                'widened': do_widen,
            })
    return {
        'file_id': m['id'],
        'file_schema_id': m['schema_id'],
        'read_schema_id': read_schema.id,
        'columns': columns,
        'missing_optional_field_ids': missing_optional,
        'widened_field_ids': widened,
    }
