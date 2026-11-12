"""本地 JSONL 的物理行读取。

行对象键为字段 ID 十进制字符串；物理行号从 0 起、覆盖全部 JSONL 行，
空行非法（仅允许文件末尾单个换行）。普通 JSON 的 NaN/Infinity 常量非法，
double 特殊值只能以 {"float": ...} 对象表达，在 schema 层识别。
"""
import json

from .errors import LakeDataError

_DIGITS = frozenset('0123456789')


def _reject_constant(token):
    raise LakeDataError(f'不接受非有限 JSON 常量: {token}')


def parse_jsonl(raw: bytes):
    """把字节资料解析为 [(physical_position, row_dict), ...]。

    只做语法层校验：对象形状、十进制字符串键、非有限常量。
    字段类型与必选/可选口径由 schema 层负责。
    """
    text = raw.decode('utf-8')
    lines = text.split('\n')
    if lines and lines[-1] == '':
        lines.pop()
    rows = []
    for position, line in enumerate(lines):
        if line == '':
            raise LakeDataError(f'物理行 {position} 为空行，空行非法')
        try:
            obj = json.loads(line, parse_constant=_reject_constant)
        except ValueError as exc:
            raise LakeDataError(f'物理行 {position} 不是合法 JSON: {exc}') from None
        if not isinstance(obj, dict):
            raise LakeDataError(f'物理行 {position} 顶层必须是对象')
        for key in obj:
            if not isinstance(key, str) or not key or any(c not in _DIGITS for c in key):
                raise LakeDataError(f'物理行 {position} 存在非字段 ID 键: {key!r}')
        rows.append((position, obj))
    return rows


def decode_rows(raw: bytes):
    """解析并把字符串字段 ID 转成 int，返回 [(position, {field_id: value})]。"""
    return [(pos, {int(key): value for key, value in row.items()})
            for pos, row in parse_jsonl(raw)]


def parse_jsonl_rows(raw: bytes):
    """解析通用 JSONL（保留字符串键），用于 position 删除记录。

    物理行号口径与 parse_jsonl 完全一致：从 0 起、空行非法、
    非有限 JSON 常量非法。
    """
    text = raw.decode('utf-8')
    lines = text.split('\n')
    if lines and lines[-1] == '':
        lines.pop()
    rows = []
    for position, line in enumerate(lines):
        if line == '':
            raise LakeDataError(f'物理行 {position} 为空行，空行非法')
        try:
            obj = json.loads(line, parse_constant=_reject_constant)
        except ValueError as exc:
            raise LakeDataError(f'物理行 {position} 不是合法 JSON: {exc}') from None
        if not isinstance(obj, dict):
            raise LakeDataError(f'物理行 {position} 顶层必须是对象')
        rows.append((position, obj))
    return rows
