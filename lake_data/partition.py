"""冻结的分区变换：identity、day、bucket[n]。

变换与规范编码已冻结，旧文件必须按自己的 partition_spec_id 解释。
bucket[n]：string 取 UTF-8 字节、long 取有符号大端 8 字节，
前面加类型字节 S/L 后做 SHA256，取前 8 字节按无符号大端整数 mod n；
null 一律变换为 null。day 只用于 timestamp，取 UTC 日。
"""
import hashlib
import re
from dataclasses import dataclass

from .errors import LakeDataError
from .schema import Special, parse_timestamp

_BUCKET_RE = re.compile(r'^bucket\[(\d+)\]$')
_TYPE_BYTE = {'string': b'S', 'long': b'L'}


@dataclass(frozen=True)
class PartitionField:
    source_id: int
    transform: str
    bucket_n: int | None


@dataclass(frozen=True)
class PartitionSpec:
    id: int
    fields: tuple

    @property
    def is_unpartitioned(self):
        return not self.fields


def build_spec(raw, schemas):
    fields = []
    seen = set()
    for pf in raw.get('fields', []):
        source_id = pf['source_id']
        if type(source_id) is not int or source_id in seen:
            raise LakeDataError(f'spec {raw.get("id")} 分区源字段缺失或重复')
        seen.add(source_id)
        transform = pf['transform']
        bucket_n = None
        if transform == 'identity' or transform == 'day':
            pass
        else:
            m = _BUCKET_RE.match(transform)
            if not m:
                raise LakeDataError(f'不支持的分区变换: {transform}')
            bucket_n = int(m.group(1))
            if bucket_n < 1:
                raise LakeDataError('bucket 模数必须 >= 1')
        # 源字段必须能在某个已知 schema 中识别；快照读取时再按行 schema 取值
        if not any(source_id in s.by_id for s in schemas.values()):
            raise LakeDataError(f'spec {raw.get("id")} 引用未知字段 ID {source_id}')
        fields.append(PartitionField(source_id, transform, bucket_n))
    return PartitionSpec(raw['id'], tuple(fields))


def _bucket_hash(value, source_type, n):
    if source_type == 'string':
        if not isinstance(value, str):
            raise LakeDataError('bucket 变换要求 string 输入')
        payload = b'S' + value.encode('utf-8')
    elif source_type == 'long':
        if type(value) is not int:
            raise LakeDataError('bucket 变换要求 long 输入')
        payload = b'L' + value.to_bytes(8, byteorder='big', signed=True)
    else:
        raise LakeDataError(f'bucket 不支持字段类型 {source_type}')
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], byteorder='big', signed=False) % n


def _transform_one(value, pf, source_type):
    if value is None:
        return None
    if pf.transform == 'identity':
        if isinstance(value, Special):
            raise LakeDataError('identity 分区不支持 double 特殊值')
        return value
    if pf.transform == 'day':
        if source_type != 'timestamp':
            raise LakeDataError('day 变换只能用于 timestamp')
        return parse_timestamp(value, pf.source_id).date().isoformat()
    # bucket
    return _bucket_hash(value, source_type, pf.bucket_n)


def transform_row(row, spec, type_lookup):
    """用给定 spec 对一行（字段 ID -> 值）求分区 tuple。

    type_lookup 为 {field_id: Field}，合并读 schema 与文件自身 schema：
    删除文件的分区过滤同样走这里，用删除文件自己的 spec 作用在候选
    数据行上，而不是直接比较不同 spec 的分区 tuple。源字段在行内缺失
    按 null 处理（必选缺失已在 schema 校验阶段失败）。
    """
    values = []
    for pf in spec.fields:
        source = type_lookup.get(pf.source_id)
        if source is None:
            raise LakeDataError(f'spec {spec.id} 的字段 {pf.source_id} 无法识别类型')
        values.append(_transform_one(row.get(pf.source_id), pf, source.type))
    return tuple(values)


def check_file_partition(row_partitions, declared, spec):
    """数据文件声明分区必须与每行自身变换结果一致。"""
    expected = tuple(declared)
    if len(expected) != len(spec.fields):
        raise LakeDataError('文件分区 tuple 宽度与 spec 不一致')
    for i, got in enumerate(row_partitions):
        if tuple(got) != expected:
            raise LakeDataError(
                f'文件声明分区 {expected} 与实际行变换 {tuple(got)} 不一致（spec {spec.id}）')
