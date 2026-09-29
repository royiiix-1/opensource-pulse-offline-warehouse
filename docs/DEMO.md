# 项目演示

1. 阅读 README 的数据范围和架构，说明四层粒度与发布边界。
2. 从 LINEAGE 展示 source SHA + 行号如何把事实关联回原始 gzip。
3. 展示缺小时阻断、补数、retry 和模型 NO_OP 的汇总证据。
4. 解释 SCD2 的观察时间、半开区间及历史关联，明确 owner 与 cohort 的含义。
5. 打开 PERFORMANCE，比较相同结果下的 Join 和布局，并同时说明文件字节增加等反例。

```bash
python scripts/demo_portfolio.py
python scripts/verify_portfolio.py --mode portable
```

第一条是已保存证据的回放，明确不冒充实时查询。完整部署上的实时验证使用 `bash scripts/verify_portfolio.sh --mode full`，会读取大量数据，应在演示前完成。
