# 监测湖表跨代分区与删除文件的快照读取

本项目保存湖仓治理的合成资料与原始配置。`lake_data.io`读取JSON清单和资产身份，`lake_data.domain`检查基本字段并给数量概况，`lake_data.cli`输出原资料摘要或按快照还原可见行。字段、数据类型、时间和版本口径见[资料约定](docs/data-contract.md)。数据中的设备、班组、主体和电量均为虚构，不连接电网、生产数仓或人员系统。

Python 3.11及以上，无第三方Python依赖。在项目根目录运行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q lake_data tests
python -m lake_data.cli inspect data/normal.json
python -m lake_data.cli read data/normal.json
```

## 快照读取层

`lake_data.reader.read_snapshot(manifest_path)` 按清单中的快照还原可见行，只实现本仓库冻结的 JSONL 湖表子集，**不冒充兼容某个湖表产品的完整格式**。每次读取先冻结快照与全部引用文件摘要，再返回：

- `snapshot`：快照 ID、读 schema、清单 sha256，以及每个引用文件的 schema/spec、分区、data_sequence、file_sequence 与资产身份（字节数、sha256、行数）；
- `read_schema` / `projections`：读 schema 字段，以及每个数据文件按**字段 ID** 做的列投影（含列重命名、旧文件缺可选列补 null、long→double 拓宽标记）；
- `visible_rows` / `deleted_rows`：可见行与被删行，均带原文件规范 URI 与删除前物理行号（从 0 起，不随过滤重排）；被删行附 `evidence`，记录实际命中的删除文件、删除序列、分区与相等键/位置；
- `counts`：文件与行计数、去重后的相等删除键数。

口径要点（与资料约定一致）：

- 列重命名不改字段 ID，读取按 ID 映射；旧文件缺新增可选列按 null 读，缺必选列或出现不支持的类型变化（如 double→long）直接失败；
- position 删除仅对 `data_sequence <= 删除 data_sequence` 的数据文件生效，equality 删除仅对 `data_sequence < 删除 data_sequence` 生效；`file_sequence` 只留档，不参与顺序判定；
- 删除文件的分区限制，是用删除文件**自己的 spec** 对候选原行做变换后比对，不直接比较不同 spec 的分区 tuple；空 spec（未分区）适用所有分区；
- 相等比较按资料口径：null 等于 null、所有 NaN 统一相等、正负零相等，与 SQL 谓词不是一套语义；
- 多份删除取并集，同一行可同时携带多条证据；position 按 `(uri, 行号)`、equality 按字段集合与键 tuple 建哈希索引，逐行只做字典查找，不重复扫描删除文件；
- 快照未引用的目录文件不会混入；position 目标 URI 不存在、行号越界、资产 sha256/字节数/行数不符等资料错误一律抛 `LakeDataError`，不会被静默当成读取完整。

## 测试资料与参考实现

`tests/make_fixtures.py` 生成 `tests/fixtures/` 下的专题资料：跨 identity/day/bucket 规范、相等删除顺序边界、同键后来插入、列重命名、long→double 拓宽、含 NaN 的新增可选列，以及 2000 行规模资料。`tests/test_reader.py` 内含一个**朴素逐行参考读取器**（无哈希索引，逐行线性扫描全部删除文件），与索引化读取器在每个快照上交叉比对可见/删除判定；并覆盖资产篡改、行号越界、目标 URI 缺失、反向类型演化、缺必选列、空行/非有限 JSON 常量、删除键上限与目录新增文件不影响重读等失败与隔离场景。

`inspect`只显示原字段概况，不修改源资料。`invalid-config.json`是基本配置错误，其余JSON为有效清单但可含需要领域操作判断的边界资料。null与缺字段应按各字段约定区分，原文件摘要绑定实际字节。

Dockerfile固定Linux基础镜像。`docker build -t lake-material .`构建，`docker run --rm lake-material`读取正常清单；`docker run --rm lake-material python -m unittest discover -s tests -v`执行已有检查。所有资料本地可读，无需账号和在线来源。
