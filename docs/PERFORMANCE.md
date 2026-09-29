# 性能实验

[完整测量表](M4_RESULTS.md)记录所有方案的三次正式样本范围、扫描文件、实际 task input bytes、shuffle 和构建成本。

采用相同代码、数据与资源配置，关闭 AQE 和自动广播，通过明确 hint 比较实际 SortMergeJoin / BroadcastHashJoin。相同逻辑的输出逐字节一致；两种文件布局另通过双向 EXCEPT ALL。

全七日与单日扫描的范围不同，其耗时比不能作为等价查询加速。小时/日级布局、Join 策略与压实实验才在相同查询结果下比较。未清空系统缓存，结果仅限本机预热后工作负载。

## 可审阅的原始测量附件

[执行计划索引](../evidence/public/plans/index.json)包含八个代表性正式测量计划及文件 SHA，按原件逐字节复制；不含逐 owner 查询结果。[输入版本清单](../evidence/public/source-manifests.json)记录 168 个原始文件的 URL、大小、行数和 SHA，便于核对数据版本，不包含事件正文。
