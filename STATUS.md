# 验证状态

本机数据与工程验收已完成；远程 CI 状态以 Actions 页面为准。

| 范围 | 结果 |
|---|---|
| 数据覆盖 | 7 个完整 UTC 日期，168 个小时 |
| 去重事件 | 29,804,303 |
| 本机实时检查 | 502 项通过 |
| 单元测试 | 25 项通过 |
| 幂等 | 相同模型输入重跑 NO_OP |
| 性能对照 | 分区、Join、小文件布局；等价结果一致 |
| 故障恢复 | 缺小时阻断与补齐、异常隔离、原生重试、ODS 内存故障恢复 |

[汇总证据](evidence/public/portfolio.json)和[本机验收摘要](evidence/public/local-validation.json)可独立审阅。原始报告的 SHA 用于关联保留的本地历史；源归档及完整日志不在仓库中。

本机完整验收与轻量 CI 是不同范围，不把 CI 成功解释为 GitHub runner 已重建七天数据。部署限制见 [LIMITATIONS](docs/LIMITATIONS.md)。
