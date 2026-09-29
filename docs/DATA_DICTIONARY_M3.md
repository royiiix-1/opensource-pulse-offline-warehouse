# M3 实际数据字典

来自已发布模型 m2cec05d7e6141168f3ac26ae；nullable 是物理 schema 属性。

## dim_repository_scd2

行数：2975375

| 字段 | 类型 | Nullable |
|---|---|---|
| repo_sk | string | True |
| repo_id | long | True |
| repo_name | string | True |
| owner_name | string | True |
| valid_from | timestamp | True |
| valid_to | timestamp | True |
| is_current | boolean | True |
| attribute_hash | string | True |

## dwd_event

行数：29804303

| 字段 | 类型 | Nullable |
|---|---|---|
| line_number | long | True |
| source_file | string | True |
| source_sha256 | string | True |
| event_id | string | True |
| event_type | string | True |
| repo_id | long | True |
| repo_name | string | True |
| actor_id | long | True |
| actor_login | string | True |
| public | boolean | True |
| event_time | timestamp | True |
| payload_json | string | True |
| run_id | string | True |
| ingested_at | timestamp | True |
| source_date | string | True |
| source_hour | integer | True |
| repo_sk | string | True |
| event_date | date | True |

## dws_organisation_daily

行数：3594146

| 字段 | 类型 | Nullable |
|---|---|---|
| owner_name | string | True |
| owner_classification | string | True |
| event_count | long | True |
| repository_count | long | True |
| active_actor_count | long | True |
| missing_actor_events | long | True |
| push_events | long | True |
| declared_commit_count | long | True |
| missing_declared_commit_events | long | True |
| coverage | string | True |
| event_date | date | True |

## dws_contributor_weekly

行数：1974453

| 字段 | 类型 | Nullable |
|---|---|---|
| actor_id | long | True |
| event_count | long | True |
| active_days | long | True |
| repository_count | long | True |
| covered_days | long | True |
| coverage | string | True |
| week_start | date | True |

## ads_contributor_retention

行数：1

| 字段 | 类型 | Nullable |
|---|---|---|
| observation_week | date | True |
| cohort_size | long | True |
| cohort_covered_days | long | True |
| observation_covered_days | long | True |
| retained_count | long | True |
| retention_rate | double | True |
| maturity | string | True |
| cohort_definition | string | True |
| cohort_week | date | True |
