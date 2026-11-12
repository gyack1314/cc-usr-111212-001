# 湖表资料口径

这是冻结JSONL湖表子集，不读Parquet或Avro。行对象键为字段ID十进制字符串，原位置以全部JSONL物理行从0计，不跳空行（空行非法）。schema支持string/long/double/timestamp，long可升double但不支持反向；timestamp为UTC ISO秒，day取UTC日。double特殊值用对象{"float":"nan"}、{"float":"+inf"}、{"float":"-inf"}表示，普通JSON非有限常量非法。可选列缺失等价读取null，显式null不可赋给required列。

partition元组按自身spec字段顺序。identity保持类型，day只用于timestamp；bucket[n]用于string/long，输入编码为string的UTF8或long的有符号大端8字节，加类型字节S/L后SHA256前8字节按无符号大端取mod n，null变换为null。文件分区必须与实际每行变换一致，删除文件分区用于过滤候选数据行，空spec适用全部行。position删除没有业务字段时schema_id只记来源，不按数据行schema解读位置记录。

删除规则以Prompt中的data_sequence为准，所有删除取并集。position目标必须在当前快照引用数据文件中且位置合法，否则资料错误；equality字段需在其schema和目标读取schema可识别。规范文件URI精确匹配，不做本地路径猜测。初始domain只核对资产身份，不应用删除。

快照读取层`lake_data.reader.read_snapshot`落实上述口径：按字段ID投影（不按显示名拼接），旧文件按自己的partition_spec_id解释分区；position取`data_sequence <=`、equality取`data_sequence <`的顺序边界，file_sequence只留档；删除分区用删除文件自身spec变换候选原行，空spec适用全部分区；相等键按null==null、NaN统一相等、正负零相等归一。读取先冻结快照与全部引用文件身份摘要，输出可见行、删除前物理行号、schema投影与命中证据；快照未引用文件不混入，目标缺失、行号越界或身份不符抛LakeDataError而不静默忽略。
