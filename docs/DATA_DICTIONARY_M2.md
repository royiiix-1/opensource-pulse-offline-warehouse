# M2 实际字段字典

发布版本：d202401012ef34518597d9e48f88b，日期：2024-01-01 UTC。由实际 Hive/Spark schema 生成。

物理 nullable 不代表业务允许缺失；业务门禁见 DATA_CONTRACTS。repository 表是观察快照，不是 SCD2。

## quarantine

实测行数：0；已登记分区：0。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| line_number | long | true | 源小时文件中的 1-based 行号 |
| raw_json | string | true | 完整事件 JSON 文本；ODS 与审计分支保留 |
| source_file | string | true | 不可变 HDFS 原始 URI |
| source_sha256 | string | true | 原始 gzip 字节 SHA-256 |
| event_id | string | true | GitHub 事件 ID；DWD 去重键 |
| event_type | string | true | 源事件类型 |
| repo_id | long | true | 稳定仓库标识 |
| repo_name | string | true | 事件中观察到的仓库名称 |
| actor_id | long | true | 公开 actor 标识，可能是自动化账号 |
| actor_login | string | true | 源公开 login，不代表自然人画像 |
| public | boolean | true | 源公开标志 |
| event_time | timestamp | true | created_at 转换为 UTC 事件时间 |
| payload_json | string | true | 完整 payload JSON |
| reason | string | true | 拒绝原因 |
| run_id | string | true | 生成 ODS 的运行版本 |
| ingested_at | timestamp | true | 该版本入库时间 |
| source_date | string | true | UTC 源文件日期 |
| source_hour | integer | true | UTC 源文件小时 |

## ods_github_event_raw

实测行数：3,879,843；已登记分区：24。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| line_number | long | true | 源小时文件中的 1-based 行号 |
| raw_json | string | true | 完整事件 JSON 文本；ODS 与审计分支保留 |
| source_file | string | true | 不可变 HDFS 原始 URI |
| source_sha256 | string | true | 原始 gzip 字节 SHA-256 |
| event_id | string | true | GitHub 事件 ID；DWD 去重键 |
| event_type | string | true | 源事件类型 |
| repo_id | long | true | 稳定仓库标识 |
| repo_name | string | true | 事件中观察到的仓库名称 |
| actor_id | long | true | 公开 actor 标识，可能是自动化账号 |
| actor_login | string | true | 源公开 login，不代表自然人画像 |
| public | boolean | true | 源公开标志 |
| event_time | timestamp | true | created_at 转换为 UTC 事件时间 |
| payload_json | string | true | 完整 payload JSON |
| run_id | string | true | 生成 ODS 的运行版本 |
| ingested_at | timestamp | true | 该版本入库时间 |
| source_date | string | true | UTC 源文件日期 |
| source_hour | integer | true | UTC 源文件小时 |

## excluded_duplicates

实测行数：6；已登记分区：3。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| line_number | long | true | 源小时文件中的 1-based 行号 |
| raw_json | string | true | 完整事件 JSON 文本；ODS 与审计分支保留 |
| source_file | string | true | 不可变 HDFS 原始 URI |
| source_sha256 | string | true | 原始 gzip 字节 SHA-256 |
| event_id | string | true | GitHub 事件 ID；DWD 去重键 |
| event_type | string | true | 源事件类型 |
| repo_id | long | true | 稳定仓库标识 |
| repo_name | string | true | 事件中观察到的仓库名称 |
| actor_id | long | true | 公开 actor 标识，可能是自动化账号 |
| actor_login | string | true | 源公开 login，不代表自然人画像 |
| public | boolean | true | 源公开标志 |
| event_time | timestamp | true | created_at 转换为 UTC 事件时间 |
| payload_json | string | true | 完整 payload JSON |
| run_id | string | true | 生成 ODS 的运行版本 |
| ingested_at | timestamp | true | 该版本入库时间 |
| canonical_source_hour | integer | true | 重复记录对应保留行的源小时 |
| canonical_line_number | long | true | 重复记录对应保留行号 |
| source_date | string | true | UTC 源文件日期 |
| source_hour | integer | true | UTC 源文件小时 |

## unknown_events

实测行数：0；已登记分区：0。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| line_number | long | true | 源小时文件中的 1-based 行号 |
| raw_json | string | true | 完整事件 JSON 文本；ODS 与审计分支保留 |
| source_file | string | true | 不可变 HDFS 原始 URI |
| source_sha256 | string | true | 原始 gzip 字节 SHA-256 |
| event_id | string | true | GitHub 事件 ID；DWD 去重键 |
| event_type | string | true | 源事件类型 |
| repo_id | long | true | 稳定仓库标识 |
| repo_name | string | true | 事件中观察到的仓库名称 |
| actor_id | long | true | 公开 actor 标识，可能是自动化账号 |
| actor_login | string | true | 源公开 login，不代表自然人画像 |
| public | boolean | true | 源公开标志 |
| event_time | timestamp | true | created_at 转换为 UTC 事件时间 |
| payload_json | string | true | 完整 payload JSON |
| run_id | string | true | 生成 ODS 的运行版本 |
| ingested_at | timestamp | true | 该版本入库时间 |
| source_date | string | true | UTC 源文件日期 |
| source_hour | integer | true | UTC 源文件小时 |

## dwd_event

实测行数：3,879,837；已登记分区：1。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| line_number | long | true | 源小时文件中的 1-based 行号 |
| source_file | string | true | 不可变 HDFS 原始 URI |
| source_sha256 | string | true | 原始 gzip 字节 SHA-256 |
| event_id | string | true | GitHub 事件 ID；DWD 去重键 |
| event_type | string | true | 源事件类型 |
| repo_id | long | true | 稳定仓库标识 |
| repo_name | string | true | 事件中观察到的仓库名称 |
| actor_id | long | true | 公开 actor 标识，可能是自动化账号 |
| actor_login | string | true | 源公开 login，不代表自然人画像 |
| public | boolean | true | 源公开标志 |
| event_time | timestamp | true | created_at 转换为 UTC 事件时间 |
| payload_json | string | true | 完整 payload JSON |
| run_id | string | true | 生成 ODS 的运行版本 |
| ingested_at | timestamp | true | 该版本入库时间 |
| source_date | string | true | UTC 源文件日期 |
| source_hour | integer | true | UTC 源文件小时 |
| event_date | date | true | UTC 事件日期分区 |

## dwd_push_event

实测行数：2,649,844；已登记分区：1。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| event_id | string | true | GitHub 事件 ID；DWD 去重键 |
| event_time | timestamp | true | created_at 转换为 UTC 事件时间 |
| repo_id | long | true | 稳定仓库标识 |
| actor_id | long | true | 公开 actor 标识，可能是自动化账号 |
| ref | string | true | Push 的 ref |
| declared_commit_count | long | true | payload.size，可为空 |
| observed_commit_array_count | integer | true | 可见 commits 数组长度，不等于完整提交数 |
| event_date | date | true | UTC 事件日期分区 |

## dwd_pull_request_event

实测行数：277,868；已登记分区：1。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| event_id | string | true | GitHub 事件 ID；DWD 去重键 |
| event_time | timestamp | true | created_at 转换为 UTC 事件时间 |
| repo_id | long | true | 稳定仓库标识 |
| actor_id | long | true | 公开 actor 标识，可能是自动化账号 |
| action | string | true | 类型内事件动作 |
| pr_number | long | true | 仓库内 PR 编号 |
| author_id | long | true | PR 作者公开 ID，区别于 actor |
| state | string | true | 源 PR/Issue 状态 |
| merged | boolean | true | 源合并布尔值，不把 closed 自动当 merged |
| created_at | timestamp | true | PR/Issue 创建时间 |
| closed_at | timestamp | true | PR/Issue 关闭时间 |
| merged_at | timestamp | true | PR 合并时间 |
| event_date | date | true | UTC 事件日期分区 |

## dwd_issue_event

实测行数：47,917；已登记分区：1。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| event_id | string | true | GitHub 事件 ID；DWD 去重键 |
| event_time | timestamp | true | created_at 转换为 UTC 事件时间 |
| repo_id | long | true | 稳定仓库标识 |
| actor_id | long | true | 公开 actor 标识，可能是自动化账号 |
| action | string | true | 类型内事件动作 |
| issue_number | long | true | 仓库内 Issue 编号 |
| state | string | true | 源 PR/Issue 状态 |
| created_at | timestamp | true | PR/Issue 创建时间 |
| closed_at | timestamp | true | PR/Issue 关闭时间 |
| event_date | date | true | UTC 事件日期分区 |

## dwd_issue_comment_event

实测行数：142,170；已登记分区：1。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| event_id | string | true | GitHub 事件 ID；DWD 去重键 |
| event_time | timestamp | true | created_at 转换为 UTC 事件时间 |
| repo_id | long | true | 稳定仓库标识 |
| actor_id | long | true | 公开 actor 标识，可能是自动化账号 |
| action | string | true | 类型内事件动作 |
| issue_number | long | true | 仓库内 Issue 编号 |
| comment_id | long | true | 评论标识 |
| event_date | date | true | UTC 事件日期分区 |

## dwd_release_event

实测行数：20,112；已登记分区：1。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| event_id | string | true | GitHub 事件 ID；DWD 去重键 |
| event_time | timestamp | true | created_at 转换为 UTC 事件时间 |
| repo_id | long | true | 稳定仓库标识 |
| actor_id | long | true | 公开 actor 标识，可能是自动化账号 |
| tag | string | true | 发布 tag |
| draft | boolean | true | 发布 draft 标志 |
| prerelease | boolean | true | 预发布标志 |
| published_at | timestamp | true | Release 发布时间 |
| event_date | date | true | UTC 事件日期分区 |

## dim_repository_observed

实测行数：590,107；已登记分区：0。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| repo_id | long | true | 稳定仓库标识 |
| repo_name | string | true | 事件中观察到的仓库名称 |
| owner_name | string | true | repo_name 前缀，未确认组织类型 |
| first_observed_at | timestamp | true | 本范围首次观察到该名称 |
| last_observed_at | timestamp | true | 本范围最后观察到该名称 |

## dws_repository_daily

实测行数：587,787；已登记分区：1。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| repo_id | long | true | 稳定仓库标识 |
| event_count | long | true | 去重后的已知事件数 |
| push_events | long | true | PushEvent 数 |
| pr_opened | long | true | PR opened 事件数 |
| pr_closed | long | true | PR closed 事件数，包含 merged |
| pr_merged | long | true | closed 且 merged=true 的事件数 |
| issue_opened | long | true | IssuesEvent opened 数 |
| issue_closed | long | true | IssuesEvent closed 数 |
| active_actors | long | true | 仓库日内 actor_id 去重，不能跨仓库相加 |
| release_events | long | true | ReleaseEvent 数 |
| watch_events | long | true | WatchEvent 数，不是存量 |
| fork_events | long | true | ForkEvent 数，不是存量 |
| coverage | string | true | COMPLETE/PARTIAL 覆盖状态 |
| observed_hours | integer | true | 覆盖的小时数 |
| event_date | date | true | UTC 事件日期分区 |

## ads_repository_activity_trend

实测行数：587,787；已登记分区：1。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| repo_id | long | true | 稳定仓库标识 |
| event_count | long | true | 去重后的已知事件数 |
| push_events | long | true | PushEvent 数 |
| active_actors | long | true | 仓库日内 actor_id 去重，不能跨仓库相加 |
| release_events | long | true | ReleaseEvent 数 |
| watch_events | long | true | WatchEvent 数，不是存量 |
| fork_events | long | true | ForkEvent 数，不是存量 |
| coverage | string | true | COMPLETE/PARTIAL 覆盖状态 |
| observed_hours | integer | true | 覆盖的小时数 |
| event_date | date | true | UTC 事件日期分区 |

## ads_pr_issue_throughput

实测行数：587,787；已登记分区：1。

| 字段 | 实际类型 | 物理 nullable | 含义 |
|---|---|---|---|
| repo_id | long | true | 稳定仓库标识 |
| pr_opened | long | true | PR opened 事件数 |
| pr_closed | long | true | PR closed 事件数，包含 merged |
| pr_merged | long | true | closed 且 merged=true 的事件数 |
| issue_opened | long | true | IssuesEvent opened 数 |
| issue_closed | long | true | IssuesEvent closed 数 |
| coverage | string | true | COMPLETE/PARTIAL 覆盖状态 |
| observed_hours | integer | true | 覆盖的小时数 |
| event_date | date | true | UTC 事件日期分区 |
