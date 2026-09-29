"""Render benchmark documentation directly from accepted evidence."""
import pathlib,sys
ROOT=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from warehouse.common import read
s=read(ROOT/'evidence/M4_SUMMARY.json');r=read(ROOT/s['evidence']/'result.json')
assert s['passed']
labels={'A_date':'日期裁剪（范围不同）','A_granularity':'相同小时查询','B_join':'相同维度关联','C_compaction':'相同全日查询'}
lines=['# M4 实测结果','',f"YARN 应用：{s['application_id']}。证据目录：{s['evidence']}。每方案一次预热、三次正式测量，全部样本保留。",'',
 '| 实验 | 方案 | 中位秒 | min—max 秒 | 扫描文件 | task 输入 MiB | shuffle 写 MiB |',
 '|---|---|---:|---:|---:|---:|---:|']
for c in s['comparisons']:
    lines.append(f"| {labels[c['experiment']]} | {c['variant']} | {c['median_seconds']:.3f} | {c['min_seconds']:.3f}—{c['max_seconds']:.3f} | {c['scan_files']} | {c['median_input_bytes']/2**20:.3f} | {c['median_shuffle_write_bytes']/2**20:.3f} |")
lines += ['', '## 正确性与结论','',
 '原表七日与一日查询范围不同，各自对账通过，只作为裁剪证据，不将时间比值包装为等价查询加速。小时粒度、Join、压实对照的全部结果文件逐字节相等；两个窄字段布局另通过双向 EXCEPT ALL，证明记录多重集一致。',
 '',f"Join 使用真实 SCD2 的确定性子集：{r['small_dimension_rows']:,} 行，JSON 序列化估计 {r['small_dimension_json_bytes']:,} 字节。事实按历史 repo_sk 关联；MERGE/BROADCAST 均由实际计划验证，不能泛化为广播任意大小维表。",'',
 '| 布局 | 文件数 | 总 MiB | 平均 MiB | 构建秒 |','|---|---:|---:|---:|---:|']
for name in ('hourly','daily_compact'):
    x=s['layouts'][name];lines.append(f"| {name} | {x['count']} | {x['total_bytes']/2**20:.3f} | {x['average_bytes']/2**20:.3f} | {x['build_seconds']:.3f} |")
lines += ['', '压实减少文件数，但总字节数增加；分区列的物理存储及行重分布同时改变，不能把变化单独归因于某一项。该全日查询中压实更快，而相同单小时查询中小时分区更快。应按查询范围选择布局，不把一种布局认定为普遍最优。',
 '', '## 失败恢复与统计过程','',
 '复核 1 月 4 日两次真实 ODS 内存失败：旧候选未发布，同一 24 小时输入恢复后完整日发布成功，前三日指针未变。内存修复通过，但 writer 阈值 1 仍触发 24 次排序回退；免排序尝试失败的结论和原日志保留。详见 evidence/m3-ods-memory-repair/acceptance.json。',
 '', '本次查询全部完成后，指标汇总脚本曾误以为 SQL description 以 collect 开头；setJobGroup 实际将其设置为实验标签。已按真实标签与第一条 SQL execution ID 关联 task 指标，排除继承标签的后续准备步骤；原异常、旧脚本、诊断及 excluded_followup_sql_ids 保留。没有重跑查询或筛选样本。',
 '', '## 环境与限制','',
 'Spark 3.5.7，YARN 两个 1 GiB 单核 executor，1 GiB driver；服务总上限 10 GiB / 6 CPU。AQE=false，自动广播=-1，shuffle=48。无 DataFrame 缓存；系统/HDFS 缓存不清空，属于本机预热后测量。只有三轮，不能声称统计显著、生产 SLA 或普遍收益。',
 '', '计划选中文件字节数包含 Parquet 全部物理列，task input bytes 是本次任务读取计数；本实验按需读列，因此两者明显不同。两项均在 JSON 保存，不能混用。',
 '',f"运行期间磁盘保守估计峰值 {s['max_conservative_disk_bytes']/2**30:.2f} GiB，低于 160 GiB；无新数据下载，发布指针不变。",'',
 '## 复现','', '先确认无其他本项目重任务运行，检查当前预算余量。以下命令会创建新证据和实验目录，旧结果不会覆盖。','',
 '```powershell',
 'wsl -d Ubuntu-26.04 --cd /mnt/d/Projects/opensource-pulse-offline-warehouse --exec bash scripts/run_m4.sh',
 '# 完成后，把输出的 evidence/m4-<run> 路径传入：',
 'wsl -d Ubuntu-26.04 --cd /mnt/d/Projects/opensource-pulse-offline-warehouse --exec .runtime/python/bin/python3.11 scripts/collect_m4_evidence.py evidence/m4-<run>',
 '```','', '官方策略说明：https://spark.apache.org/docs/3.5.7/sql-performance-tuning.html','']
(ROOT/'docs/M4_RESULTS.md').write_text('\n'.join(lines))
print('docs/M4_RESULTS.md written from accepted evidence')
