# 数据合同

所有日期与小时均为 UTC；GH Archive 文件小时不补零（0..23）。事件时间用于 event_date；source_date/source_hour 独立保留，不能以任务时间替代。只允许 HTTPS data.gharchive.org 固定模板，不接受用户提供任意 URL。

## 文件与 manifest

每个输入包含 schema_version、source_url、source_date、source_hour、compressed_bytes、uncompressed_bytes、sha256、line_count、acquired_at、run_id。SHA-256 为本地所收字节的完整性标识，不是数据源签名。必须读到 gzip EOF 校验 CRC，拒绝空文件/截断 gzip；失败文件保留在 attempt quarantine，不能发布 raw 成功状态。每次下载先 HEAD 和磁盘预算检查，实际 GET 流量和解压字节同样限额，禁止以 HEAD 代替硬上限。

## 行路由及对账

原始非空/空行均计数。坏 JSON、非对象、缺失/非法 event_id、created_at、repo.id、repo.name、公有标志不为 true、跨 source hour 的记录进入 quarantine，记录 reason/source SHA/line number/run_id。未知 event_type 保留完整原始 JSON 并分类 unknown；不得丢弃未知字段。可选 payload 字段允许 null；计数 null 与 0 不等价。

ODS 仅包含结构有效行，event_id 不为空，但采集重复在 ODS 可见。原始行数 = ODS 行数 + 结构拒绝行数。ODS 行数 = DWD 保留事件 + 重复排除 + unknown + 业务拒绝 + 明确排除。重复 event_id 内容完全相同去重；冲突内容拒绝并阻止发布，不任意 last-write-wins。DWD 通用事件登记表保存所有有效已知事件，专用事实表为它的互斥事件类型投影，不能把事实表重复相加。

质量报告包含每项门禁 pass/fail、实际值、阈值、输入摘要、转换摘要、run_id、application_id、开始/结束时间和发布状态。原始 gzip 损坏是文件级失败；坏行进入 quarantine 后按零坏行默认门槛阻止发布，若未来放宽必须版本化阈值并记录理由。

## 可观察属性

repo_id 是仓库自然键。repo_name 的 owner 部分叫 owner_name，不能推断它一定是 organisation。SCD2 只描述公开事件观察到的名称；valid_from 是观察时间，非真实改名时间。区间 [valid_from, valid_to)，末端 null。同一时间冲突名称隔离并阻止维度发布；相同名称连续观察不生成新版本。repo_sk 由 repo_id、valid_from 和属性摘要确定性生成。历史回填不能只更新当前记录。

PR closed 与 merged 分开：merged 必须 payload.pull_request.merged=true 且相应动作语义有效；缺字段不能推断合并。commit 数优先使用源计数字段，记录口径及缺失数；payload.commits 数组长度不声称完整提交数量。

完整日必须 expected_hours=24；切片显式 PARTIAL，记录 selected_hours=[0]。周汇总与留存必须标记输入覆盖范围，未成熟窗口不能填 0 或推算。