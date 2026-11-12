"""湖仓原始资料与配置读取，以及按快照还原可见行的读取层。

读取入口：
    from lake_data.reader import read_snapshot
    result = read_snapshot('path/to/manifest.json')

只实现 docs/data-contract.md 冻结的 JSONL 湖表子集，
不冒充兼容某个湖表产品的完整格式。资料不满足口径时抛出
lake_data.errors.LakeDataError，不静默降级为“读取完整”。
"""
from .errors import LakeDataError
from .reader import read_snapshot

__all__ = ['read_snapshot', 'LakeDataError']
