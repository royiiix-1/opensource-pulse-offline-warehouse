# 架构

系统以源小时为采集单元、UTC 日期为日级发布单元，以一组选定完整日期构建多日模型。

1. Python 校验 URL、预算、文件长度、gzip CRC 与 SHA，保存 manifest。
2. 原始 gzip 写入 HDFS 的 date/hour/SHA 路径，禁止覆盖成功对象。
3. Spark Core 为源记录保留稳定行号；Spark SQL 解析异构事件，写入分区化 Hive 外部表。
4. 日级质量门禁核对小时覆盖、重复冲突、异常路由及跨层指标。
5. 多日模型重建 repository SCD2，以事件时间关联 repo_sk，计算 owner 日汇总、贡献者周汇总与留存成熟度。
6. 候选文件及 Hive 元数据全部检查通过后，原子替换本地版本指针。

## 编排

日级 Airflow DAG：prepare → source check → acquisition → manifest validation → HDFS raw → ODS → DWD → quality gate → DWS → ADS → reconciliation → publication → audit。

多日模型由独立 DAG 执行。日期互斥和模型锁阻止冲突写入；失败记录保留。对完整历史数据的重新校验本身有 I/O 成本，NO_OP 不意味着零读取。

## 存储与一致性

ODS 与审计表按 source_date/source_hour 分区，日级事实和汇总按 event_date 分区。每个版本具有独立路径和 Hive 数据库，消费者通过指针固定版本。Hive 多次 ALTER 与 HDFS rename 不构成分布式事务；只有最终指针提交后，候选才成为正式读取版本。

当前验证环境为独立 WSL 用户服务。HMS 使用 Derby，Airflow 使用 SQLite/SequentialExecutor，计算由 YARN 执行。部署是单机实验，不提供高可用或多租户保证。
