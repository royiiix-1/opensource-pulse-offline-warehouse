# M4 实测结果

YARN 应用：application_1790607104350_0060。证据目录：evidence/m4-20260929T095943Z7aebc84d。每方案一次预热、三次正式测量，全部样本保留。

| 实验 | 方案 | 中位秒 | min—max 秒 | 扫描文件 | task 输入 MiB | shuffle 写 MiB |
|---|---|---:|---:|---:|---:|---:|
| 日期裁剪（范围不同） | all_dates | 10.205 | 9.836—10.829 | 336 | 130.874 | 0.242 |
| 日期裁剪（范围不同） | one_date | 1.791 | 1.781—1.800 | 48 | 14.263 | 0.038 |
| 相同小时查询 | daily_compact | 0.642 | 0.640—0.683 | 4 | 29.719 | 0.003 |
| 相同小时查询 | hourly | 0.381 | 0.376—0.390 | 1 | 1.149 | 0.002 |
| 相同全日查询 | hourly | 1.048 | 1.016—1.054 | 24 | 18.867 | 0.003 |
| 相同全日查询 | daily_compact | 0.814 | 0.790—0.831 | 4 | 27.378 | 0.003 |
| 相同维度关联 | sort_merge | 19.302 | 18.974—19.695 | 337 | 412.009 | 375.266 |
| 相同维度关联 | broadcast | 14.108 | 13.419—14.224 | 337 | 412.009 | 1.157 |

## 正确性与结论

原表七日与一日查询范围不同，各自对账通过，只作为裁剪证据，不将时间比值包装为等价查询加速。小时粒度、Join、压实对照的全部结果文件逐字节相等；两个窄字段布局另通过双向 EXCEPT ALL，证明记录多重集一致。

Join 使用真实 SCD2 的确定性子集：11,743 行，JSON 序列化估计 1,286,503 字节。事实按历史 repo_sk 关联；MERGE/BROADCAST 均由实际计划验证，不能泛化为广播任意大小维表。

| 布局 | 文件数 | 总 MiB | 平均 MiB | 构建秒 |
|---|---:|---:|---:|---:|
| hourly | 24 | 42.259 | 1.761 | 6.243 |
| daily_compact | 4 | 58.385 | 14.596 | 4.331 |

压实减少文件数，但总字节数增加；分区列的物理存储及行重分布同时改变，不能把变化单独归因于某一项。该全日查询中压实更快，而相同单小时查询中小时分区更快。应按查询范围选择布局，不把一种布局认定为普遍最优。

## 失败恢复与统计过程

复核 1 月 4 日两次真实 ODS 内存失败：旧候选未发布，同一 24 小时输入恢复后完整日发布成功，前三日指针未变。内存修复通过，但 writer 阈值 1 仍触发 24 次排序回退；免排序尝试失败的结论和原日志保留。详见 evidence/m3-ods-memory-repair/acceptance.json。

指标按原生 SQL execution ID 归属，排除继承实验标签的后续准备步骤；预热与正式测量分开，全部样本保留。

## 环境与限制

Spark 3.5.7，YARN 两个 1 GiB 单核 executor，1 GiB driver；服务总上限 10 GiB / 6 CPU。AQE=false，自动广播=-1，shuffle=48。无 DataFrame 缓存；系统/HDFS 缓存不清空，属于本机预热后测量。只有三轮，不能声称统计显著、生产 SLA 或普遍收益。

计划选中文件字节数包含 Parquet 全部物理列，task input bytes 是本次任务读取计数；本实验按需读列，因此两者明显不同。两项均在 JSON 保存，不能混用。

运行期间磁盘保守估计峰值 128.40 GiB，低于 160 GiB；无新数据下载，发布指针不变。

## 复现

先确认无其他本项目重任务运行，检查当前预算余量。以下命令会创建新证据和实验目录，旧结果不会覆盖。

```powershell
wsl -d Ubuntu-26.04 --cd /mnt/d/Projects/opensource-pulse-offline-warehouse --exec bash scripts/run_m4.sh
# 完成后，把输出的 evidence/m4-<run> 路径传入：
wsl -d Ubuntu-26.04 --cd /mnt/d/Projects/opensource-pulse-offline-warehouse --exec .runtime/python/bin/python3.11 scripts/collect_m4_evidence.py evidence/m4-<run>
```

官方策略说明：https://spark.apache.org/docs/3.5.7/sql-performance-tuning.html
