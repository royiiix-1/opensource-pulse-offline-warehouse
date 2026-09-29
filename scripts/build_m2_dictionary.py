import json,pathlib
R=pathlib.Path(__file__).resolve().parents[1]
a=json.loads((R/"build/control-v2/published/2024-01-01.json").read_text())
meaning={
"event_id":"GitHub 事件 ID；DWD 去重键","event_type":"源事件类型","actor_id":"公开 actor 标识，可能是自动化账号","actor_login":"源公开 login，不代表自然人画像",
"repo_id":"稳定仓库标识","repo_name":"事件中观察到的仓库名称","public":"源公开标志","event_time":"created_at 转换为 UTC 事件时间",
"payload_json":"完整 payload JSON","raw_json":"完整事件 JSON 文本；ODS 与审计分支保留","line_number":"源小时文件中的 1-based 行号",
"source_date":"UTC 源文件日期","source_hour":"UTC 源文件小时","source_file":"不可变 HDFS 原始 URI","source_sha256":"原始 gzip 字节 SHA-256",
"run_id":"生成 ODS 的运行版本","ingested_at":"该版本入库时间","event_date":"UTC 事件日期分区",
"canonical_source_hour":"重复记录对应保留行的源小时","canonical_line_number":"重复记录对应保留行号",
"ref":"Push 的 ref","declared_commit_count":"payload.size，可为空","observed_commit_array_count":"可见 commits 数组长度，不等于完整提交数",
"action":"类型内事件动作","pr_number":"仓库内 PR 编号","author_id":"PR 作者公开 ID，区别于 actor","state":"源 PR/Issue 状态",
"merged":"源合并布尔值，不把 closed 自动当 merged","created_at":"PR/Issue 创建时间","closed_at":"PR/Issue 关闭时间","merged_at":"PR 合并时间",
"issue_number":"仓库内 Issue 编号","comment_id":"评论标识","tag":"发布 tag","draft":"发布 draft 标志","prerelease":"预发布标志",
"published_at":"Release 发布时间","owner_name":"repo_name 前缀，未确认组织类型","first_observed_at":"本范围首次观察到该名称","last_observed_at":"本范围最后观察到该名称",
"event_count":"去重后的已知事件数","push_events":"PushEvent 数","pr_opened":"PR opened 事件数","pr_closed":"PR closed 事件数，包含 merged",
"pr_merged":"closed 且 merged=true 的事件数","issue_opened":"IssuesEvent opened 数","issue_closed":"IssuesEvent closed 数",
"active_actors":"仓库日内 actor_id 去重，不能跨仓库相加","release_events":"ReleaseEvent 数","watch_events":"WatchEvent 数，不是存量","fork_events":"ForkEvent 数，不是存量",
"coverage":"COMPLETE/PARTIAL 覆盖状态","observed_hours":"覆盖的小时数","reason":"拒绝原因"
}
lines=["# M2 实际字段字典","",f"发布版本：{a['release']}，日期：2024-01-01 UTC。由实际 Hive/Spark schema 生成。","",
       "物理 nullable 不代表业务允许缺失；业务门禁见 DATA_CONTRACTS。repository 表是观察快照，不是 SCD2。",""]
for name,t in a["tables"].items():
    lines += ["## "+name,"",f"实测行数：{t['rows']:,}；已登记分区：{len(t['partitions'])}。","",
              "| 字段 | 实际类型 | 物理 nullable | 含义 |","|---|---|---|---|"]
    for f in t["schema"]["fields"]:
        typ=f["type"] if isinstance(f["type"],str) else json.dumps(f["type"],ensure_ascii=False)
        lines.append("| "+f["name"]+" | "+typ+" | "+str(f["nullable"]).lower()+" | "+meaning.get(f["name"],"见模型 SQL")+" |")
    lines.append("")
(R/"docs/DATA_DICTIONARY_M2.md").write_text("\n".join(lines),encoding="utf-8")
