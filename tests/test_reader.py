"""读取层的口径测试与朴素逐行参考实现交叉验证。

参考读取器故意不建任何索引：每个候选行都扫描全部删除文件，
与生产读取器（哈希索引、每行字典查找）是两套独立代码路径，
两者在同快照上的可见/删除判定必须完全一致。
"""
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from lake_data.errors import LakeDataError
from lake_data.io import load
from lake_data.jsonl_io import decode_rows, parse_jsonl_rows
from lake_data.partition import build_spec, transform_row
from lake_data.reader import read_snapshot
from lake_data.schema import (
    Special,
    build_schema,
    eq_key,
    eq_tuple,
)

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / 'data'
FIXTURES = ROOT / 'tests' / 'fixtures'


# ---------------------------------------------------------- 朴素参考读取器

class ReferenceReader:
    """逐行全扫描参考实现，仅用于与索引化读取器对照。"""

    def __init__(self, manifest_path):
        self.path = Path(manifest_path)
        self.v, _ = load(self.path)
        self.schemas = {s['id']: build_schema(s) for s in self.v['schemas']}
        self.specs = {s['id']: build_spec(s, self.schemas)
                      for s in self.v['partition_specs']}
        self.files = {f['id']: f for f in self.v['files']}
        self.read_schema = self.schemas[self.v['snapshot']['schema_id']]
        self._row_cache = {}

    def _rows(self, entry):
        # 只缓存解析结果，判定仍然逐行线性扫描，不引入任何索引
        if entry['id'] not in self._row_cache:
            raw = (self.path.parent / entry['asset']['path']).read_bytes()
            if entry['kind'] == 'position':
                self._row_cache[entry['id']] = parse_jsonl_rows(raw)
            else:
                self._row_cache[entry['id']] = decode_rows(raw)
        return self._row_cache[entry['id']]

    def _project(self, entry, row):
        fs = self.schemas[entry['schema_id']]
        out = {}
        for rf in self.read_schema.fields:
            ff = fs.by_id.get(rf.id)
            if ff is None:
                if rf.required:
                    raise LakeDataError('参考实现：缺必选列')
                out[rf.id] = None
            elif rf.id not in row:
                if rf.required:
                    raise LakeDataError('参考实现：缺必选值')
                out[rf.id] = None
            else:
                val = row[rf.id]
                if val is not None and isinstance(val, dict) and set(val) == {'float'}:
                    val = Special(val['float'])
                if val is not None and ff.type == 'long' and rf.type == 'double' \
                        and type(val) is int:
                    val = float(val)
                out[rf.id] = val
        return out

    def _types(self, entry):
        fs = self.schemas[entry['schema_id']]
        return {**self.read_schema.by_id, **fs.by_id}

    def _spec_match(self, delete_entry, data_entry, decoded_row):
        spec = self.specs[delete_entry['spec_id']]
        if not spec.fields:
            return True
        got = transform_row(decoded_row, spec, self._types(data_entry))
        return got == tuple(delete_entry['partition'])

    def run(self):
        snap = self.v['snapshot']
        data_entries = [self.files[i] for i in snap['file_ids']
                        if self.files[i]['kind'] == 'data']
        deletes = [self.files[i] for i in snap['file_ids']
                   if self.files[i]['kind'] in ('position', 'equality')]
        visible, deleted = [], []
        for de in data_entries:
            for pos, row in self._rows(de):
                projected = self._project(de, row)
                hits = []
                for x in deletes:
                    if not self._spec_match(x, de, row):
                        continue
                    if x['kind'] == 'position':
                        if not (de['data_sequence'] <= x['data_sequence']):
                            continue
                        for rec, drow in self._rows(x):
                            if drow['file'] == de['uri'] and drow['position'] == pos:
                                hits.append(('position', x['id'], rec))
                    else:
                        if not (de['data_sequence'] < x['data_sequence']):
                            continue
                        fids = tuple(x['field_ids'])
                        if any(f not in self._types(de) for f in fids):
                            continue
                        target = eq_tuple(tuple(projected.get(f) for f in fids))
                        for rec, drow in self._rows(x):
                            key = eq_tuple(tuple(drow.get(f) for f in fids))
                            if key == target:
                                hits.append(('equality', x['id'], rec))
                token = (de['id'], pos)
                if hits:
                    deleted.append(token)
                else:
                    visible.append(token)
        return visible, deleted


def _tokens(result):
    vis = [(r['file_id'], r['position']) for r in result['visible_rows']]
    dele = [(r['file_id'], r['position']) for r in result['deleted_rows']]
    return vis, dele


# ------------------------------------------------------------------ 测试

class ProvidedMaterialTests(unittest.TestCase):
    def test_three_manifests_expected_outcomes(self):
        # normal：相等 seq5 严格顺序 + 位置 seq2 非严格顺序 + null==null
        r = read_snapshot(DATA / 'normal.json')
        vis, dele = _tokens(r)
        self.assertEqual(vis, [('d2', 0), ('d2', 1)])
        self.assertEqual(dele, [('d1', 0), ('d1', 1), ('d1', 2)])
        ids = {e['delete_file_id'] for row in r['deleted_rows'] for e in row['evidence']}
        self.assertEqual(ids, {'e1', 'p1'})

        # 同键后来插入：d2 的 'a'（seq5）不被 seq5 相等删除误伤
        d2a = next(x for x in r['visible_rows'] if x['file_id'] == 'd2' and x['position'] == 0)
        self.assertEqual(d2a['row']['1'], 'a')
        self.assertEqual(d2a['row']['4'], 'ok')

        # 边界清单：相等删除降到 seq2，d1（seq2）不再严格小于 -> 复活
        rb = read_snapshot(DATA / 'equality-sequence-boundary.json')
        vis_b, dele_b = _tokens(rb)
        self.assertEqual(vis_b, [('d1', 0), ('d1', 2), ('d2', 0), ('d2', 1)])
        self.assertEqual(dele_b, [('d1', 1)])

        # 旧快照不引用 d2：目录里有 d2 资产也不能混入
        ro = read_snapshot(DATA / 'older-file-set.json')
        self.assertEqual(ro['snapshot']['file_ids'], ['d1', 'e1', 'p1'])
        vis_o, _ = _tokens(ro)
        self.assertEqual(vis_o, [])

    def test_matches_naive_reference_reader(self):
        for name in ('normal.json', 'equality-sequence-boundary.json',
                     'older-file-set.json'):
            with self.subTest(manifest=name):
                result = read_snapshot(DATA / name)
                ref_vis, ref_del = ReferenceReader(DATA / name).run()
                self.assertEqual(_tokens(result), (ref_vis, ref_del))

    def test_column_rename_maps_by_field_id(self):
        r = read_snapshot(DATA / 'normal.json')
        d1_proj = next(p for p in r['projections'] if p['file_id'] == 'd1')
        col1 = next(c for c in d1_proj['columns'] if c['field_id'] == 1)
        self.assertEqual(col1['file_name'], 'device')
        self.assertEqual(col1['read_name'], 'equipment')  # 改名不改 ID
        self.assertIn(4, d1_proj['missing_optional_field_ids'])
        d1_rows = [x for x in r['deleted_rows'] if x['file_id'] == 'd1']
        self.assertTrue(all(x['row']['4'] is None for x in d1_rows))

    def test_invalid_config_still_rejected(self):
        with self.assertRaises((LakeDataError, KeyError, TypeError, ValueError)):
            read_snapshot(DATA / 'invalid-config.json')


class CrossSpecFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = read_snapshot(FIXTURES / 'cross-spec.json')

    def test_expected_visibility(self):
        vis, dele = _tokens(self.result)
        self.assertEqual(vis, [
            ('d1', 1), ('d1', 2),
            ('d2', 0),
            ('d3', 0), ('d3', 2),
        ])
        self.assertEqual(dele, [('d1', 0), ('d1', 3), ('d2', 1), ('d3', 1)])

    def test_matches_naive_reference_reader(self):
        ref_vis, ref_del = ReferenceReader(FIXTURES / 'cross-spec.json').run()
        self.assertEqual(_tokens(self.result), (ref_vis, ref_del))

    def test_evidence_is_concrete(self):
        evidence = {(r['file_id'], r['position']): r['evidence']
                    for r in self.result['deleted_rows']}
        self.assertEqual([e['kind'] for e in evidence[('d1', 0)]], ['equality'])
        self.assertEqual(evidence[('d1', 0)][0]['delete_file_id'], 'eq-day')
        self.assertEqual(evidence[('d1', 3)][0]['key'], [None])  # null==null
        pos = evidence[('d2', 1)][0]
        self.assertEqual(pos['kind'], 'position')
        self.assertEqual((pos['target_uri'], pos['target_position']),
                         ('lake://cs-d2', 1))
        bkt = evidence[('d3', 1)][0]
        self.assertEqual(bkt['delete_file_id'], 'eq-bucket')
        self.assertEqual(bkt['key'], [7])

    def test_partition_restrictions_are_spec_relative(self):
        # eq-day 的 day 分区只作用在 2026-11-08：d3 同键 'a' 不受影响
        d3a = next(r for r in self.result['visible_rows']
                   if r['file_id'] == 'd3' and r['position'] == 0)
        self.assertEqual(d3a['row']['1'], 'a')
        # eq-bucket 的 bucket 限制：d1 的 reading=10 与 7 不同桶，不被删
        d1b = next(r for r in self.result['visible_rows']
                   if r['file_id'] == 'd1' and r['position'] == 1)
        self.assertEqual(d1b['row']['2'], 20)

    def test_file_sequence_is_archival_only(self):
        # eq-day 的 file_sequence=90 低于数据文件 d1 的 101，仍按 data_sequence 生效
        frozen = {f['id']: f for f in self.result['snapshot']['files']}
        self.assertEqual(frozen['eq-day']['file_sequence'], 90)
        self.assertTrue(any(
            r['file_id'] == 'd1' and r['position'] == 0
            for r in self.result['deleted_rows']))

    def test_new_optional_column_and_special_double(self):
        nan_row = next(r for r in self.result['visible_rows']
                       if r['file_id'] == 'd3' and r['position'] == 2)
        self.assertEqual(nan_row['row']['5'], {'float': 'nan'})
        d1_rows = [r for r in self.result['visible_rows'] if r['file_id'] == 'd1']
        self.assertTrue(all(r['row']['5'] is None for r in d1_rows))


class WidenFixtureTests(unittest.TestCase):
    def test_long_to_double_widening(self):
        r = read_snapshot(FIXTURES / 'widen.json')
        self.assertEqual(r['projections'][0]['widened_field_ids'], [2])
        for row in r['visible_rows']:
            self.assertIsInstance(row['row']['2'], float)
        self.assertEqual([row['row']['2'] for row in r['visible_rows']],
                         [10.0, 20.0, -7.0])

    def test_matches_naive_reference_reader(self):
        result = read_snapshot(FIXTURES / 'widen.json')
        ref_vis, ref_del = ReferenceReader(FIXTURES / 'widen.json').run()
        self.assertEqual(_tokens(result), (ref_vis, ref_del))


class ScaleAndIndexTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = read_snapshot(FIXTURES / 'scale.json')

    def test_scale_outcome_and_physical_positions(self):
        r = self.result
        # 前 1000 个物理行被相等/位置删除全部覆盖；后 1000 行原样可见
        self.assertEqual(r['counts']['visible'], 1000)
        self.assertEqual(r['counts']['deleted'], 1000)
        self.assertEqual(min(x['position'] for x in r['visible_rows']), 1000)
        # 可见行保留删除前物理行号，没有边过滤边重排
        self.assertEqual([x['position'] for x in r['visible_rows']],
                         list(range(1000, 2000)))

    def test_union_keeps_all_hits_for_same_row(self):
        row0 = next(x for x in self.result['deleted_rows']
                    if x['file_id'] == 'd1' and x['position'] == 0)
        kinds = sorted(e['kind'] + ':' + e['delete_file_id']
                       for e in row0['evidence'])
        self.assertEqual(kinds, ['equality:e1', 'position:p2'])

    def test_indexes_are_hash_lookups_not_per_row_scans(self):
        # 直接重放内部索引构造，确认 position 按 (uri,position) 字典、
        # equality 按字段集合字典组织，行级判定只需字典查找
        from pathlib import Path as _P
        import lake_data.reader as rm
        from lake_data.io import load as _load
        from lake_data.schema import build_schema, build_projection
        from lake_data.partition import build_spec

        v, _ = _load(FIXTURES / 'scale.json')
        schemas = {s['id']: build_schema(s) for s in v['schemas']}
        specs = {s['id']: build_spec(s, schemas) for s in v['partition_specs']}
        read_schema = schemas[v['snapshot']['schema_id']]
        by_id = {f['id']: f for f in v['files']}
        data_target = rm._build_data_file(
            by_id['d1'], FIXTURES, schemas, specs, read_schema)
        pos_entries, eq_entries = [], []
        for fid in v['snapshot']['file_ids']:
            e = by_id[fid]
            if e['kind'] == 'position':
                raw = (FIXTURES / e['asset']['path']).read_bytes()
                e = dict(e)
                e['_raw'] = raw
                pos_entries.append(rm._build_position_delete(
                    e, specs, {'lake://scale-d1': data_target}))
            elif e['kind'] == 'equality':
                eq_entries.append(
                    rm._build_equality_delete(e, FIXTURES, schemas, specs,
                                              read_schema))
        index = rm._build_index(pos_entries, eq_entries, v['max_delete_keys'])
        self.assertIn(('lake://scale-d1', 0), index.position)
        self.assertIn(('lake://scale-d1', 999), index.position)
        self.assertNotIn(('lake://scale-d1', 1000), index.position)
        self.assertEqual(set(index.equality), {(1,)})
        self.assertEqual(len(index.equality[(1,)]), 500)
        self.assertEqual(index.distinct_equality_keys, 500)

    def test_matches_naive_reference_reader(self):
        ref_vis, ref_del = ReferenceReader(FIXTURES / 'scale.json').run()
        self.assertEqual(_tokens(self.result), (ref_vis, ref_del))


class PartitionEdgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def _write(self, name, rows):
        import hashlib
        raw = ''.join(json.dumps(r, separators=(',', ':')) + '\n'
                      for r in rows).encode('utf8')
        (self.dir / name).write_bytes(raw)
        return {'path': name, 'bytes': len(raw),
                'sha256': hashlib.sha256(raw).hexdigest(), 'rows': len(rows)}

    def _manifest(self, data_rows, delete_rows, delete_spec_id, delete_partition,
                  delete_seq=5):
        schemas = [
            {'id': 1, 'fields': [
                {'id': 1, 'name': 'device', 'type': 'string', 'required': False},
                {'id': 2, 'name': 'reading', 'type': 'long', 'required': True},
                {'id': 3, 'name': 'at', 'type': 'timestamp', 'required': True},
            ]},
        ]
        specs = [
            {'id': 1, 'fields': [{'source_id': 3, 'transform': 'day'}]},
            {'id': 2, 'fields': []},
        ]
        files = [
            {'id': 'd1', 'kind': 'data', 'uri': 'lake://edge-d1',
             'schema_id': 1, 'spec_id': 1, 'partition': ['2026-11-08'],
             'data_sequence': 2, 'file_sequence': 401,
             'asset': self._write('edge-d1.jsonl', data_rows)},
            {'id': 'e1', 'kind': 'equality', 'schema_id': 1,
             'spec_id': delete_spec_id, 'partition': delete_partition,
             'data_sequence': delete_seq, 'file_sequence': 402,
             'field_ids': [1],
             'asset': self._write('edge-eq.jsonl', delete_rows)},
        ]
        manifest = {
            'schemas': schemas, 'partition_specs': specs, 'files': files,
            'snapshot': {'id': 'edge-s1', 'schema_id': 1,
                         'file_ids': ['d1', 'e1']},
            'max_delete_keys': 10000,
        }
        p = self.dir / 'edge.json'
        p.write_text(json.dumps(manifest, ensure_ascii=False), encoding='utf8')
        return p

    def test_utc_day_boundary_uses_transformed_row_not_tuple_compare(self):
        # 23:59:50 与 00:00:05 跨 UTC 日；删除限定 2026-11-08
        rows = [
            {'1': 'a', '2': 1, '3': '2026-11-08T23:59:50Z'},
            {'1': 'a', '2': 2, '3': '2026-11-09T00:00:05Z'},
        ]
        # 数据文件自身 spec 1 要求整日一致，这里两行不同日 -> 必须报错，
        # 证明文件分区按每行变换强校验，而不是只比对声明 tuple
        with self.assertRaises(LakeDataError):
            read_snapshot(self._manifest(rows, [{'1': 'a'}], 1, ['2026-11-08']))

    def test_equality_delete_partition_mismatch_does_not_apply(self):
        rows = [
            {'1': 'a', '2': 1, '3': '2026-11-08T23:00:00Z'},
            {'1': 'b', '2': 2, '3': '2026-11-08T23:30:00Z'},
        ]
        # 删除文件声明 2026-11-09，与候选行变换出的 2026-11-08 不符 -> 全保留
        path = self._manifest(rows, [{'1': 'a'}, {'1': 'b'}], 1, ['2026-11-09'])
        r = read_snapshot(path)
        self.assertEqual(len(r['visible_rows']), 2)
        self.assertEqual(r['deleted_rows'], [])

    def test_unpartitioned_delete_applies_across_partitions(self):
        rows = [
            {'1': 'a', '2': 1, '3': '2026-11-08T23:00:00Z'},
            {'1': 'b', '2': 2, '3': '2026-11-08T23:30:00Z'},
        ]
        path = self._manifest(rows, [{'1': 'a'}], 2, [])
        r = read_snapshot(path)
        vis, dele = _tokens(r)
        self.assertEqual(vis, [('d1', 1)])
        self.assertEqual(dele, [('d1', 0)])

    def test_equality_sequence_equal_does_not_delete(self):
        rows = [{'1': 'a', '2': 1, '3': '2026-11-08T23:00:00Z'}]
        # 删除与数据同为 data_sequence 2：相等删除要求严格小于 -> 不生效
        path = self._manifest(rows, [{'1': 'a'}], 2, [], delete_seq=2)
        r = read_snapshot(path)
        self.assertEqual(len(r['visible_rows']), 1)


class EqualityKeySemanticsTests(unittest.TestCase):
    def test_null_nan_signed_zero(self):
        self.assertEqual(eq_key(None), eq_key(None))
        self.assertEqual(eq_key(float('nan')), eq_key(float('nan')))
        self.assertEqual(eq_key(Special('nan')), eq_key(float('nan')))
        self.assertEqual(eq_key(0.0), eq_key(-0.0))
        self.assertNotEqual(eq_key(0.0), eq_key(0))      # 类型不混同
        self.assertNotEqual(eq_key(1), eq_key(1.0))
        self.assertNotEqual(eq_key(Special('+inf')), eq_key(Special('-inf')))
        self.assertEqual(eq_tuple([None, 0.0]), eq_tuple([None, -0.0]))

    def test_special_value_roundtrip(self):
        self.assertEqual(Special('+inf').to_jsonable(), {'float': '+inf'})
        with self.assertRaises(LakeDataError):
            Special('bogus')


class BucketEncodingTests(unittest.TestCase):
    def test_frozen_encoding_matches_manifest(self):
        manifest = json.loads((FIXTURES / 'cross-spec.json').read_text('utf8'))
        spec = build_spec(
            next(s for s in manifest['partition_specs'] if s['id'] == 4),
            {s['id']: build_schema(s) for s in manifest['schemas']})
        types = {s['id']: build_schema(s) for s in manifest['schemas']}
        type_lookup = {}
        for sch in types.values():
            type_lookup.update(sch.by_id)
        for value, expected in manifest['bucket_values'].items():
            got = transform_row({2: int(value)}, spec, type_lookup)
            self.assertEqual(got, (expected,))

    def test_null_transforms_to_null_and_string_bucket_differs(self):
        manifest = json.loads((FIXTURES / 'cross-spec.json').read_text('utf8'))
        schemas = {s['id']: build_schema(s) for s in manifest['schemas']}
        spec_b = build_spec(
            next(s for s in manifest['partition_specs'] if s['id'] == 4), schemas)
        type_lookup = {}
        for sch in schemas.values():
            type_lookup.update(sch.by_id)
        self.assertEqual(transform_row({2: None}, spec_b, type_lookup), (None,))
        spec_i = build_spec(
            next(s for s in manifest['partition_specs'] if s['id'] == 3), schemas)
        self.assertEqual(transform_row({1: 'c'}, spec_i, type_lookup), ('c',))


class FailureModeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        shutil.copytree(FIXTURES, self.dir, dirs_exist_ok=True)
        shutil.copytree(DATA, self.dir, dirs_exist_ok=True)

    def _mutate_manifest(self, name, mutate):
        path = self.dir / name
        v, _ = load(path)
        mutate(v)
        path.write_text(json.dumps(v, ensure_ascii=False), encoding='utf8')
        return path

    def test_asset_tamper_caught_before_target_lookup(self):
        # 直接篡改 position 资产但不更新身份：sha256 校验必须先拦下，
        # 不会落到“目标不存在”的分支被静默处理
        path = self.dir / 'cross-spec.json'
        asset = self.dir / 'p-identity.jsonl'
        asset.write_bytes(asset.read_bytes() + b'{"file":"lake://ghost","position":0}\n')
        with self.assertRaises(LakeDataError):
            read_snapshot(path)

    def test_position_target_uri_unknown_is_error(self):
        def mutate(v):
            (self.dir / 'p-identity.jsonl').write_text(
                '{"file":"lake://ghost","position":0}\n', encoding='utf8')
            for f in v['files']:
                if f['id'] == 'p-identity':
                    raw = (self.dir / 'p-identity.jsonl').read_bytes()
                    import hashlib
                    f['asset']['sha256'] = hashlib.sha256(raw).hexdigest()
                    f['asset']['bytes'] = len(raw)
        path = self._mutate_manifest('cross-spec.json', mutate)
        with self.assertRaises(LakeDataError):
            read_snapshot(path)

    def test_position_out_of_range_is_error(self):
        def mutate(v):
            (self.dir / 'p-identity.jsonl').write_text(
                '{"file":"lake://cs-d2","position":99}\n', encoding='utf8')
            import hashlib
            raw = (self.dir / 'p-identity.jsonl').read_bytes()
            for f in v['files']:
                if f['id'] == 'p-identity':
                    f['asset']['sha256'] = hashlib.sha256(raw).hexdigest()
                    f['asset']['bytes'] = len(raw)
        path = self._mutate_manifest('cross-spec.json', mutate)
        with self.assertRaises(LakeDataError):
            read_snapshot(path)

    def test_asset_tamper_is_error(self):
        path = self.dir / 'cross-spec.json'
        target = self.dir / 'd1.jsonl'
        target.write_bytes(target.read_bytes() + b'{"1":"z","2":99,"3":"2026-11-09T00:00:00Z"}\n')
        with self.assertRaises(LakeDataError):
            read_snapshot(path)

    def test_unsupported_double_to_long_evolution(self):
        # 文件 schema 改为 double、读 schema 保持 long：反向演化必须失败
        def mutate(v):
            for s in v['schemas']:
                if s['id'] == 1:
                    for f in s['fields']:
                        if f['id'] == 2:
                            f['type'] = 'double'
                if s['id'] == 2:
                    for f in s['fields']:
                        if f['id'] == 2:
                            f['type'] = 'long'
        path = self._mutate_manifest('widen.json', mutate)
        with self.assertRaises(LakeDataError):
            read_snapshot(path)

    def test_missing_required_column_is_error(self):
        def mutate(v):
            for s in v['schemas']:
                if s['id'] == 2:
                    s['fields'].append(
                        {'id': 9, 'name': 'batch', 'type': 'string', 'required': True})
        path = self._mutate_manifest('widen.json', mutate)
        with self.assertRaises(LakeDataError):
            read_snapshot(path)

    def test_equality_field_unknown_to_read_schema_is_error(self):
        def mutate(v):
            for f in v['files']:
                if f['id'] == 'eq-day':
                    f['field_ids'] = [99]
        path = self._mutate_manifest('cross-spec.json', mutate)
        with self.assertRaises(LakeDataError):
            read_snapshot(path)

    def _refresh_asset_identity(self, manifest_name, file_id):
        import hashlib
        path = self.dir / manifest_name
        v, _ = load(path)
        for f in v['files']:
            if f['id'] == file_id:
                raw = (self.dir / f['asset']['path']).read_bytes()
                f['asset']['sha256'] = hashlib.sha256(raw).hexdigest()
                f['asset']['bytes'] = len(raw)
        path.write_text(json.dumps(v, ensure_ascii=False), encoding='utf8')
        return path

    def test_blank_line_and_non_finite_constant_are_errors(self):
        good = (self.dir / 'd2.jsonl').read_bytes()
        # 同步刷新资产身份，确保错误来自 JSONL 物理行口径而非 sha256
        (self.dir / 'd2.jsonl').write_bytes(good + b'\n')
        path = self._refresh_asset_identity('cross-spec.json', 'd2')
        with self.assertRaises(LakeDataError):
            read_snapshot(path)
        (self.dir / 'd2.jsonl').write_bytes(good + b'{"1":NaN}\n')
        path = self._refresh_asset_identity('cross-spec.json', 'd2')
        with self.assertRaises(LakeDataError):
            read_snapshot(path)

    def test_snapshot_reference_unknown_file_is_error(self):
        def mutate(v):
            v['snapshot']['file_ids'].append('nope')
        path = self._mutate_manifest('cross-spec.json', mutate)
        with self.assertRaises(LakeDataError):
            read_snapshot(path)

    def test_delete_key_cap_enforced(self):
        def mutate(v):
            v['max_delete_keys'] = 1
        path = self._mutate_manifest('cross-spec.json', mutate)
        with self.assertRaises(LakeDataError):
            read_snapshot(path)


class DirectoryNoiseTests(unittest.TestCase):
    def test_unreferenced_files_never_mix_in(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            shutil.copytree(DATA, d, dirs_exist_ok=True)
            before = read_snapshot(d / 'older-file-set.json')
            # 目录里新增一个未被任何快照引用的数据文件与同名干扰清单
            (d / 'rogue.jsonl').write_text(
                '{"1":"x","2":1,"3":"2026-11-10T00:00:00Z"}\n', encoding='utf8')
            (d / 'rogue.json').write_text('{"unrelated": true}', encoding='utf8')
            after = read_snapshot(d / 'older-file-set.json')
            self.assertEqual(
                [r['file_id'] for r in after['visible_rows']],
                [r['file_id'] for r in before['visible_rows']])
            referenced = {f['asset']['path'] for f in after['snapshot']['files']}
            self.assertNotIn('rogue.jsonl', referenced)

    def test_repeat_read_is_deterministic(self):
        first = read_snapshot(DATA / 'normal.json')
        second = read_snapshot(DATA / 'normal.json')
        self.assertEqual(json.dumps(first, sort_keys=True),
                         json.dumps(second, sort_keys=True))


class FreezeBlockTests(unittest.TestCase):
    def test_freeze_carries_identity_and_sequences(self):
        r = read_snapshot(DATA / 'normal.json')
        freeze = r['snapshot']
        self.assertEqual(freeze['snapshot_id'], 's1')
        self.assertEqual(freeze['snapshot_schema_id'], 2)
        self.assertEqual(len(freeze['manifest_sha256']), 64)
        by_id = {f['id']: f for f in freeze['files']}
        self.assertEqual(by_id['d1']['data_sequence'], 2)
        self.assertEqual(by_id['d1']['file_sequence'], 10)
        self.assertEqual(by_id['p1']['asset']['sha256'],
                         '6f1d59093bef7e6b5fc6c4d1b4b5843690d9fae90d0649cafca7606ed1682730')
        # 计数与实际输出一致
        self.assertEqual(r['counts']['visible'], len(r['visible_rows']))
        self.assertEqual(r['counts']['deleted'], len(r['deleted_rows']))


if __name__ == '__main__':
    unittest.main(verbosity=2)
