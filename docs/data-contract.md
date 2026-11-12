# 湖表资料口径

这是冻结JSONL湖表子集，不读Parquet或Avro。行对象键为字段ID十进制字符串，原位置以全部JSONL物理行从0计，不跳空行（空行非法）。schema支持string/long/double/timestamp，long可升double但不支持反向；timestamp为UTC ISO秒，day取UTC日。double特殊值用对象{"float":"nan"}、{"float":"+inf"}、{"float":"-inf"}表示，普通JSON非有限常量非法。可选列缺失等价读取null，显式null不可赋给required列。

partition元组按自身spec字段顺序。identity保持类型，day只用于timestamp；bucket[n]用于string/long，输入编码为string的UTF8或long的有符号大端8字节，加类型字节S/L后SHA256前8字节按无符号大端取mod n，null变换为null。文件分区必须与实际每行变换一致，删除文件分区用于过滤候选数据行，空spec适用全部行。position删除没有业务字段时schema_id只记来源，不按数据行schema解读位置记录。

删除规则以Prompt中的data_sequence为准，所有删除取并集。position目标必须在当前快照引用数据文件中且位置合法，否则资料错误；equality字段需在其schema和目标读取schema可识别。规范文件URI精确匹配，不做本地路径猜测。初始domain只核对资产身份，不应用删除。
