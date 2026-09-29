# 数据模型

所有发布表使用 Hive 外部表与 Parquet/Snappy。日级与多日模型具有独立版本，不通过追加副本累加数据规模。

| 表 | 粒度 | 分区与说明 |
|---|---|---|
| ods_github_event_raw | source SHA + line_number | source_date / source_hour，保留 raw_json |
| dwd_event | event_id | event_date；多日版本增加历史 repo_sk |
| dwd_push_event | PushEvent | event_date；声明提交数与可见数组长度分开 |
| dwd_pull_request_event | PullRequestEvent | event_date；opened/closed/明确 merged 分开 |
| dwd_issue_event | IssuesEvent | event_date；动作事件数，不是 Issue 存量 |
| dwd_issue_comment_event | IssueCommentEvent | event_date；与 Issue 本体分开 |
| dwd_release_event | ReleaseEvent | event_date；保留 tag、draft、prerelease 与时间 |
| dim_repository_observed | repo_id / repo_name | 日内名称观察，不等于 SCD2 |
| dim_repository_scd2 | repo_sk；repo_id / valid_from | 名称观察历史，区间 [valid_from, valid_to) |
| dws_repository_daily | event_date / repo_id | 仓库活动与 PR/Issue 动作汇总 |
| dws_organisation_daily | event_date / owner_name | 名称沿用 organisation，但分类明确为 observed_owner_prefix |
| dws_contributor_weekly | week_start / actor_id | UTC 周一开始；活动天数、事件数、仓库数及覆盖 |
| ads_repository_activity_trend | event_date / repo_id | 活动事件指标与完整性 |
| ads_pr_issue_throughput | event_date / repo_id | opened/closed/merged 动作数量 |
| ads_contributor_retention | cohort_week / observation_week | 样本首次观察 cohort，未成熟窗口为 null |

完整实物字段见 [日级字典](DATA_DICTIONARY_M2.md)与[多日字典](DATA_DICTIONARY_M3.md)。字典中的 nullable 是物理 schema 属性，不替代业务质量约束。

SCD2 的 valid_from 是观察时间，不是真实改名时间。事实按 repo_id 与事件时间关联，不能只连 current 行。owner 层的 distinct actor 从 DWD 独立重算，不能累加仓库级 distinct。未知字段保留在 ODS 原文中，异常与重复记录保留审计分支。
