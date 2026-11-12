"""字段 ID 投影、类型口径与相等删除的键语义。

读取一律按字段 ID 映射，列的显示名称（如 device 改名为 equipment）
不参与拼接。相等比较采用资料自己的口径：null 等于 null、所有 NaN
统一相等、正负零相等，这与 SQL 谓词语义不同，由 eq_key 单独实现。
"""
import datetime
import math
from dataclasses import dataclass

from .errors import LakeDataError

_PRIMITIVE_TYPES = ('string', 'long', 'double', 'timestamp')
# 仅允许 long -> double 这一种拓宽；反向及其他组合都不支持。
_PROMOTION = {('long', 'double')}
_FLOAT_TAGS = {'nan': math.nan, '+inf': math.inf, '-inf': -math.inf}


@dataclass(frozen=True)
class Field:
    id: int
    name: str
    type: str
    required: bool


@dataclass(frozen=True)
class Schema:
    id: int
    fields: tuple
    by_id: dict


class Special:
    """{"float": "nan" | "+inf" | "-inf"} 的内部表示。

    保留独立类型而不是直接塞进 float，是为了让 +inf/-inf 不会与普通
    数值混淆；输出时再按冻结对象形式还原。
    """

    __slots__ = ('tag', 'value')

    def __init__(self, tag):
        if tag not in _FLOAT_TAGS:
            raise LakeDataError(f'未知 double 特殊值标签: {tag}')
        self.tag = tag
        self.value = _FLOAT_TAGS[tag]

    def __repr__(self):
        return f'Special({self.tag!r})'

    def to_jsonable(self):
        return {'float': self.tag}

    def __eq__(self, other):
        return isinstance(other, Special) and other.tag == self.tag

    def __hash__(self):
        return hash(('Special', self.tag))


def build_schema(raw):
    fields = []
    seen = set()
    for f in raw.get('fields', []):
        fid = f['id']
        if type(fid) is not int or fid in seen:
            raise LakeDataError(f'schema {raw.get("id")} 字段 ID 缺失或重复')
        seen.add(fid)
        ftype = f['type']
        if ftype not in _PRIMITIVE_TYPES:
            raise LakeDataError(f'不支持的字段类型: {ftype}')
        if not isinstance(f.get('required'), bool):
            raise LakeDataError(f'字段 {fid} 的 required 必须是布尔')
        name = f['name']
        if not isinstance(name, str) or not name:
            raise LakeDataError(f'字段 {fid} 名称非法')
        fields.append(Field(fid, name, ftype, f['required']))
    if not fields:
        raise LakeDataError(f'schema {raw.get("id")} 没有字段')
    return Schema(raw['id'], tuple(fields), {f.id: f for f in fields})


def _check_scalar(value, ftype, field_id):
    """校验单个物理值是否符合文件自身 schema 的字段类型。"""
    if ftype == 'string':
        if not isinstance(value, str):
            raise LakeDataError(f'字段 {field_id} 应为 string')
    elif ftype == 'long':
        # bool 是 int 子类，必须排除
        if type(value) is not int:
            raise LakeDataError(f'字段 {field_id} 应为 long')
    elif ftype == 'double':
        if isinstance(value, Special):
            return
        if type(value) is int:
            return  # long 可作为 double 输入
        if type(value) is not float:
            raise LakeDataError(f'字段 {field_id} 应为 double')
    elif ftype == 'timestamp':
        if not isinstance(value, str):
            raise LakeDataError(f'字段 {field_id} 应为 timestamp 字符串')
        parse_timestamp(value, field_id)


def parse_timestamp(value, field_id=''):
    """UTC ISO 秒时间戳，返回 aware datetime（UTC）。"""
    text = value
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        dt = datetime.datetime.fromisoformat(text)
    except ValueError:
        raise LakeDataError(f'字段 {field_id} 时间戳非法: {value}') from None
    if dt.tzinfo is None:
        raise LakeDataError(f'字段 {field_id} 时间戳缺少时区: {value}')
    if dt.microsecond:
        raise LakeDataError(f'字段 {field_id} 时间戳只支持秒精度: {value}')
    return dt.astimezone(datetime.timezone.utc)


def decode_field_value(value, ftype, field_id):
    """把 JSONL 原始值按文件字段类型解释（识别 float 特殊值对象）。

    显式 null 原样返回；必选列不允许 null 的口径由 project_row/
    数据文件校验阶段负责，本函数只检查非空值的物理类型。
    """
    if value is None:
        return None
    if isinstance(value, dict) and set(value) == {'float'}:
        tag = value['float']
        if tag not in _FLOAT_TAGS:
            raise LakeDataError(f'字段 {field_id} float 标签非法: {tag}')
        value = Special(tag)
    _check_scalar(value, ftype, field_id)
    return value


def build_projection(file_schema, read_schema):
    """生成文件 schema -> 读 schema 的字段 ID 投影描述。

    返回 (projected_fields, plan)，plan 为按读 schema 字段顺序的
    (field_id, file_type|None, read_field, widen_to_double) 列表。
    旧文件缺少读 schema 新增的可选列按 null 读；缺必选列或存在
    不支持的类型变化直接失败。
    """
    plan = []
    projected = []
    for rf in read_schema.fields:
        ff = file_schema.by_id.get(rf.id)
        if ff is None:
            if rf.required:
                raise LakeDataError(
                    f'文件 schema {file_schema.id} 缺少读 schema 必选字段 {rf.id}')
            plan.append((rf.id, None, rf, False))
            continue
        if ff.type == rf.type:
            widen = False
        elif (ff.type, rf.type) in _PROMOTION:
            widen = True
        else:
            raise LakeDataError(
                f'字段 {rf.id} 不支持的类型变化: {ff.type} -> {rf.type}')
        plan.append((rf.id, ff.type, rf, widen))
        projected.append(rf.id)
    return tuple(projected), tuple(plan)


def project_row(row, plan, file_schema):
    """按投影计划把一个物理行转成读 schema 形状，并做必选/类型校验。"""
    out = {}
    for fid, file_type, read_field, widen in plan:
        if file_type is None:
            out[fid] = None  # 旧文件缺失的可选列
            continue
        if fid not in row:
            if read_field.required:
                raise LakeDataError(f'schema {file_schema.id} 物理行缺必选字段 {fid}')
            out[fid] = None
            continue
        value = decode_field_value(row[fid], file_type, fid)
        if value is None:
            if read_field.required:
                raise LakeDataError(f'schema {file_schema.id} 必选字段 {fid} 显式为 null')
            out[fid] = None
            continue
        if widen and type(value) is int:
            value = float(value)
        out[fid] = value
    # 文件里出现读 schema 无法识别的字段属于资料错误
    unknown = set(row) - set(file_schema.by_id)
    if unknown:
        raise LakeDataError(f'schema {file_schema.id} 物理行含未知字段 ID: {sorted(unknown)}')
    return out


def to_output_value(value):
    """把内部值转成可 json.dumps(allow_nan=False) 的形式。"""
    if isinstance(value, Special):
        return value.to_jsonable()
    return value


class _EqKey:
    """相等删除的归一化键片段。"""
    __slots__ = ('kind', 'value')

    def __init__(self, kind, value):
        self.kind = kind
        self.value = value

    def __eq__(self, other):
        return isinstance(other, _EqKey) and self.kind == other.kind and self.value == other.value

    def __hash__(self):
        return hash((self.kind, self.value))

    def __repr__(self):
        return f'_EqKey({self.kind!r}, {self.value!r})'


def eq_key(value):
    """资料口径的相等键：null==null、NaN 统一相等、+0==-0。

    不同物理类型保留不同 kind，避免 Python 自身 1 == 1.0 这类
    跨类型混同；而 ±0 与 NaN 按资料口径归一。
    """
    if value is None:
        return _EqKey('null', None)
    if isinstance(value, Special):
        if value.tag == 'nan':
            return _EqKey('nan', None)
        return _EqKey('special', value.tag)
    if type(value) is bool:
        return _EqKey('bool', value)
    if type(value) is int:
        return _EqKey('long', value)
    if type(value) is float:
        if math.isnan(value):
            return _EqKey('nan', None)
        if value == 0.0:
            return _EqKey('double-zero', 0)
        return _EqKey('double', value)
    if isinstance(value, str):
        return _EqKey('string', value)
    # 时间戳在资料里就是字符串，归一化前已通过类型校验
    return _EqKey('other', value)


def eq_tuple(values):
    return tuple(eq_key(v) for v in values)
