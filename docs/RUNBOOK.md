# 运行手册

## 轻量验证

Linux / Python 3.11：`python scripts/verify_portfolio.py --mode portable`。无需集群或数据下载，报告保存至 build/validation/<run>。

## 完整部署边界

已验证环境：Windows + Ubuntu-26.04 WSL2，Hadoop 3.3.6、Spark 3.5.7、Hive 3.1.3、Airflow 2.10.5、Python 3.11.14、Temurin 8u472-b08。发行包与依赖摘要见 config/runtime-lock.json 和 Airflow 锁文件。

Windows 入口绑定 `D:\Projects\opensource-pulse-offline-warehouse`，WSL 发行版名为 `Ubuntu-26.04`。其他主机需调整并重新验收，不能把便携单测通过等同于完整部署可用。初始服务入口为 scripts/bootstrap_airflow.sh；安装之前先检查磁盘、端口、版本锁和下载预算。

原部署使用专属 HDFS/YARN/HMS 端口、osp-offline 用户服务和 /var/tmp/osp-offline-1000 缓存。参考上限为 20 GiB 累计公网下载、160 GiB 存储、10 GiB 服务内存及 6 CPU；新环境必须重新估算，配置中的历史预留不等于新主机已发生流量。

## 日级和模型任务

在已配置的项目目录中：

```powershell
.\scripts\run_day.ps1 -Date 2024-01-07 -CacheOnly
.\scripts\run_day.ps1 -Date 2024-01-07 -Hour 23 -CacheOnly
.\scripts\run_day.ps1 -StartDate 2024-01-06 -EndDate 2024-01-07 -CacheOnly
```

```bash
bash scripts/run_m3_model.sh
bash scripts/verify_portfolio.sh --mode full
```

CacheOnly 约束输入获取，不保证代码变更后的输出仍 NO_OP。文件字节参与模型身份，注释或运维代码变化也可能保守触发新版本。日期范围最多七天；任务超时与预算仍生效。

日级指针在 build/control-v2/published，多日模型指针在 build/control-m3/published.json。不要与同日期活动任务并行提交，也不要直接读取 staging 当作成功结果。

## 故障处理

先检查 DagRun 终态，再定位阶段报告、YARN application 与资源记录。缺小时先补齐再校验全天；坏文件隔离；源小时多版本默认拒绝歧义；同刻名称冲突阻断 SCD2。保留旧失败，不删除记录或放宽质量门槛来使验收通过。

停止服务只针对本项目：`systemctl --user stop osp-offline-airflow.service osp-offline-cluster.service`。不做全局容器清理、WSL shutdown 或跨项目数据操作。
