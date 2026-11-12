"""生成跨分区规范/删除顺序/重命名专题的冻结测试资料。

资料输出到 tests/fixtures 下，资产 sha256、字节数与行数在生成时
写入清单，读取层据此做身份核对。该脚本只服务测试，不参与运行时读取。
"""
import hashlib
import json
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent / 'fixtures'


def bucket4_long(value):
    """与 lake_data.partition 冻结编码保持一致的 bucket[4] 计算。"""
    payload = b'L' + value.to_bytes(8, byteorder='big', signed=True)
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], byteorder='big', signed=False) % 4


def write_jsonl(folder, name, rows):
    raw = ''.join(json.dumps(r, ensure_ascii=False, separators=(',', ':')) + '\n'
                  for r in rows).encode('utf-8')
    (folder / name).write_bytes(raw)
    return {'path': name, 'bytes': len(raw),
            'sha256': hashlib.sha256(raw).hexdigest(), 'rows': len(rows)}


def write_manifest(folder, name, payload):
    (folder / name).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


# ------------------------------------------------------------- 主专题资料

def build_cross_spec(folder):
    """identity/day/bucket 三规范 + 顺序边界 + NaN/±0 + 重命名同键。"""
    schemas = [
        {'id': 1, 'fields': [
            {'id': 1, 'name': 'device', 'type': 'string', 'required': False},
            {'id': 2, 'name': 'reading', 'type': 'long', 'required': True},
            {'id': 3, 'name': 'at', 'type': 'timestamp', 'required': True},
        ]},
        {'id': 2, 'fields': [
            {'id': 1, 'name': 'equipment', 'type': 'string', 'required': False},
            {'id': 2, 'name': 'reading', 'type': 'long', 'required': True},
            {'id': 3, 'name': 'at', 'type': 'timestamp', 'required': True},
            {'id': 4, 'name': 'quality', 'type': 'string', 'required': False},
            {'id': 5, 'name': 'score', 'type': 'double', 'required': False},
        ]},
    ]
    specs = [
        {'id': 1, 'fields': [{'source_id': 3, 'transform': 'day'}]},
        {'id': 2, 'fields': []},
        {'id': 3, 'fields': [{'source_id': 1, 'transform': 'identity'}]},
        {'id': 4, 'fields': [{'source_id': 2, 'transform': 'bucket[4]'}]},
    ]

    # d1：day 分区（schema 1），含 null 设备键
    d1_rows = [
        {'1': 'a', '2': 10, '3': '2026-11-08T23:00:00Z'},
        {'1': 'b', '2': 20, '3': '2026-11-08T23:30:00Z'},
        {'1': 'b', '2': 21, '3': '2026-11-08T23:40:00Z'},
        {'1': None, '2': 30, '3': '2026-11-08T23:50:00Z'},
    ]
    # d2：identity 分区（schema 2，重命名后的同字段 ID），整文件同分区 'c'
    d2_rows = [
        {'1': 'c', '2': 50, '3': '2026-11-09T02:00:00Z', '4': None},
        {'1': 'c', '2': 55, '3': '2026-11-09T02:30:00Z', '4': 'ok'},
    ]
    # d3：未分区，含同键 'a' 后来插入与 double 特殊值（字段 5 为新增可选列）
    d3_rows = [
        {'1': 'a', '2': 40, '3': '2026-11-09T01:00:00Z', '4': 'ok', '5': 1.5},
        {'1': 'g1', '2': 7, '3': '2026-11-09T03:00:00Z', '4': 'ok'},
        {'1': 'g2', '2': 8, '3': '2026-11-09T04:00:00Z', '4': None,
         '5': {'float': 'nan'}},
    ]

    # eq-day：equality(1) + day 分区限制，只删 2026-11-08 的 'a'/null
    eq_day_rows = [{'1': 'a'}, {'1': None}]
    # eq-bucket：equality(2) + bucket[4] 分区限制；7 的桶只在 d3 行上成立
    bucket_of_7 = bucket4_long(7)
    eq_bucket_rows = [{'2': 7}, {'2': 10}]
    # p-identity：position + identity 分区限制，只对设备 'c' 分区文件生效
    p_rows = [{'file': 'lake://cs-d2', 'position': 1}]
    def data_entry(fid, uri, rows, schema_id, spec_id, partition, seq, fseq):
        return {'id': fid, 'kind': 'data', 'uri': uri,
                'schema_id': schema_id, 'spec_id': spec_id,
                'partition': partition, 'data_sequence': seq,
                'file_sequence': fseq,
                'asset': write_jsonl(folder, f'{fid}.jsonl', rows)}

    def delete_entry(fid, kind, rows, schema_id, spec_id, partition, seq, fseq,
                     field_ids=None):
        e = {'id': fid, 'kind': kind, 'schema_id': schema_id,
             'spec_id': spec_id, 'partition': partition,
             'data_sequence': seq, 'file_sequence': fseq,
             'asset': write_jsonl(folder, f'{fid}.jsonl', rows)}
        if field_ids is not None:
            e['field_ids'] = field_ids
        return e

    files = [
        data_entry('d1', 'lake://cs-d1', d1_rows, 1, 1,
                   ['2026-11-08'], 2, 101),
        data_entry('d2', 'lake://cs-d2', d2_rows, 2, 3,
                   ['c'], 5, 102),
        data_entry('d3', 'lake://cs-d3', d3_rows, 2, 2,
                   [], 6, 103),
        # file_sequence=90 故意低于更老数据文件的 101，证明顺序只看 data_sequence
        delete_entry('eq-day', 'equality', eq_day_rows, 1, 1,
                     ['2026-11-08'], 5, 90, field_ids=[1]),
        delete_entry('eq-bucket', 'equality', eq_bucket_rows, 1, 4,
                     [bucket_of_7], 7, 105, field_ids=[2]),
        delete_entry('p-identity', 'position', p_rows, 1, 3,
                     ['c'], 6, 106),
    ]
    manifest = {
        'schemas': schemas,
        'partition_specs': specs,
        'files': files,
        'snapshot': {'id': 'cs-s1', 'schema_id': 2,
                     'file_ids': ['d1', 'd2', 'd3', 'eq-day', 'eq-bucket',
                                  'p-identity']},
        'max_delete_keys': 10000,
        'bucket_values': {'7': bucket_of_7,
                          '10': bucket4_long(10),
                          '30': bucket4_long(30)},
    }
    write_manifest(folder, 'cross-spec.json', manifest)


def build_widen(folder):
    """long -> double 拓宽资料（反向拓宽与缺必选列在测试里临时构造）。"""
    schema_old = {'id': 1, 'fields': [
        {'id': 1, 'name': 'device', 'type': 'string', 'required': False},
        {'id': 2, 'name': 'reading', 'type': 'long', 'required': True},
        {'id': 3, 'name': 'at', 'type': 'timestamp', 'required': True},
    ]}
    schema_double = {'id': 2, 'fields': [
        {'id': 1, 'name': 'equipment', 'type': 'string', 'required': False},
        {'id': 2, 'name': 'reading', 'type': 'double', 'required': True},
        {'id': 3, 'name': 'at', 'type': 'timestamp', 'required': True},
        {'id': 4, 'name': 'quality', 'type': 'string', 'required': False},
    ]}
    rows = [
        {'1': 'a', '2': 10, '3': '2026-11-08T23:00:00Z'},
        {'1': 'b', '2': 20, '3': '2026-11-08T23:30:00Z'},
        {'1': 'c', '2': -7, '3': '2026-11-08T23:40:00Z'},
    ]
    entry = {'id': 'd1', 'kind': 'data', 'uri': 'lake://widen-d1',
             'schema_id': 1, 'spec_id': 1, 'partition': [],
             'data_sequence': 2, 'file_sequence': 201,
             'asset': write_jsonl(folder, 'widen-d1.jsonl', rows)}
    manifest = {
        'schemas': [schema_old, schema_double],
        'partition_specs': [{'id': 1, 'fields': []},
                            {'id': 2, 'fields': []}],
        'files': [entry],
        'snapshot': {'id': 'w-s1', 'schema_id': 2, 'file_ids': ['d1']},
        'max_delete_keys': 10000,
    }
    write_manifest(folder, 'widen.json', manifest)


def build_scale(folder):
    """大样本：2000 数据行 + 500 相等键 + 500 位置删除，验证索引化口径。"""
    schemas = [
        {'id': 1, 'fields': [
            {'id': 1, 'name': 'device', 'type': 'string', 'required': False},
            {'id': 2, 'name': 'reading', 'type': 'long', 'required': True},
            {'id': 3, 'name': 'at', 'type': 'timestamp', 'required': True},
        ]},
        {'id': 2, 'fields': [
            {'id': 1, 'name': 'equipment', 'type': 'string', 'required': False},
            {'id': 2, 'name': 'reading', 'type': 'long', 'required': True},
            {'id': 3, 'name': 'at', 'type': 'timestamp', 'required': True},
            {'id': 4, 'name': 'quality', 'type': 'string', 'required': False},
        ]},
    ]
    specs = [{'id': 1, 'fields': []}, {'id': 2, 'fields': []}]
    n = 2000
    data_rows = [
        {'1': f'dev-{i}', '2': i,
         '3': '2026-11-09T00:00:00Z'}
        for i in range(n)
    ]
    # 相等删除：前 500 个偶数设备（位置 0,2,...,998），seq 5 严格大于数据 seq 2
    eq_rows = [{'1': f'dev-{i}'} for i in range(0, 1000, 2)]
    # 位置删除：前 500 个奇数物理行（1,3,...,999），seq 2 非严格生效
    pos_rows = [{'file': 'lake://scale-d1', 'position': i}
                for i in range(1, 1000, 2)]
    # 第二份位置删除再打位置 0：与相等删除并集命中同一行，证据应同时保留
    pos2_rows = [{'file': 'lake://scale-d1', 'position': 0}]
    files = [
        {'id': 'd1', 'kind': 'data', 'uri': 'lake://scale-d1',
         'schema_id': 1, 'spec_id': 1, 'partition': [],
         'data_sequence': 2, 'file_sequence': 301,
         'asset': write_jsonl(folder, 'scale-d1.jsonl', data_rows)},
        {'id': 'e1', 'kind': 'equality', 'schema_id': 1, 'spec_id': 2,
         'partition': [], 'data_sequence': 5, 'file_sequence': 302,
         'field_ids': [1],
         'asset': write_jsonl(folder, 'scale-eq.jsonl', eq_rows)},
        {'id': 'p1', 'kind': 'position', 'schema_id': 1, 'spec_id': 2,
         'partition': [], 'data_sequence': 2, 'file_sequence': 303,
         'asset': write_jsonl(folder, 'scale-pos.jsonl', pos_rows)},
        {'id': 'p2', 'kind': 'position', 'schema_id': 1, 'spec_id': 2,
         'partition': [], 'data_sequence': 2, 'file_sequence': 304,
         'asset': write_jsonl(folder, 'scale-pos2.jsonl', pos2_rows)},
    ]
    manifest = {
        'schemas': schemas,
        'partition_specs': specs,
        'files': files,
        'snapshot': {'id': 'scale-s1', 'schema_id': 2,
                     'file_ids': ['d1', 'e1', 'p1', 'p2']},
        'max_delete_keys': 10000,
        'scale': {'rows': n, 'equality_keys': len(eq_rows),
                  'position_deletes': len(pos_rows) + len(pos2_rows),
                  'first_visible_position': 1000},
    }
    write_manifest(folder, 'scale.json', manifest)


def main():
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    build_cross_spec(FIXTURE_DIR)
    build_widen(FIXTURE_DIR)
    build_scale(FIXTURE_DIR)
    print('fixtures written to', FIXTURE_DIR)


if __name__ == '__main__':
    main()
