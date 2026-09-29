import json, pathlib, os, subprocess, xml.etree.ElementTree as ET
R=pathlib.Path(__file__).resolve().parents[1]
rt=R/".runtime"
conf=R/"config/runtime"
conf.mkdir(exist_ok=True)
state=R/"build/cluster"
state.mkdir(parents=True,exist_ok=True)
native=pathlib.Path(json.loads((R/"config/native-cache.json").read_text())["root"])
if native.is_symlink(): raise RuntimeError("native state path is a symlink")
native.mkdir(mode=0o700,exist_ok=True)
filesystem=subprocess.check_output(["findmnt","-n","-o","FSTYPE","-T",str(native)],text=True).strip()
if filesystem in {"tmpfs","ramfs","9p","drvfs"}: raise RuntimeError("Spark/YARN cache requires a disk-backed POSIX filesystem")
for d in ["yarn-local","yarn-log","spark-local"]: (native/d).mkdir(exist_ok=True)
for d in ["logs","pids","nn","dn","yarn-local","yarn-log","tmp","metastore"]:
    (state/d).mkdir(exist_ok=True)
def xml(name, values):
    root=ET.Element("configuration")
    for k,v in values.items():
        prop=ET.SubElement(root,"property")
        ET.SubElement(prop,"name").text=k
        ET.SubElement(prop,"value").text=str(v)
    ET.indent(root)
    ET.ElementTree(root).write(conf/name,encoding="utf-8",xml_declaration=True)
xml("core-site.xml",{"fs.defaultFS":"hdfs://127.0.0.1:19000","hadoop.tmp.dir":state/"tmp"})
xml("hdfs-site.xml",{
"dfs.replication":1,"dfs.namenode.name.dir":"file://"+str(state/"nn"),
"dfs.datanode.data.dir":"file://"+str(state/"dn"),
"dfs.namenode.rpc-address":"127.0.0.1:19000",
"dfs.namenode.http-address":"127.0.0.1:19870",
"dfs.datanode.address":"127.0.0.1:19866",
"dfs.datanode.http.address":"127.0.0.1:19864",
"dfs.datanode.ipc.address":"127.0.0.1:19867",
"dfs.namenode.safemode.min.datanodes":1})
xml("yarn-site.xml",{
"yarn.resourcemanager.hostname":"localhost",
"yarn.resourcemanager.scheduler.class":"org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler",
"yarn.resourcemanager.bind-host":"127.0.0.1",
"yarn.resourcemanager.address":"127.0.0.1:18032",
"yarn.resourcemanager.scheduler.address":"127.0.0.1:18030",
"yarn.resourcemanager.resource-tracker.address":"127.0.0.1:18031",
"yarn.resourcemanager.admin.address":"127.0.0.1:18033",
"yarn.resourcemanager.webapp.address":"127.0.0.1:18088",
"yarn.nodemanager.hostname":"localhost",
"yarn.nodemanager.bind-host":"127.0.0.1",
"yarn.nodemanager.address":"127.0.0.1:18041",
"yarn.nodemanager.localizer.address":"127.0.0.1:18040",
"yarn.nodemanager.webapp.address":"127.0.0.1:18042",
"yarn.nodemanager.resource.memory-mb":6144,
"yarn.nodemanager.resource.cpu-vcores":4,
"yarn.scheduler.minimum-allocation-mb":512,
"yarn.scheduler.maximum-allocation-mb":6144,
"yarn.nodemanager.vmem-check-enabled":"false",
"yarn.nodemanager.local-dirs":native/"yarn-local",
"yarn.nodemanager.log-dirs":native/"yarn-log",
"yarn.nodemanager.env-whitelist":"JAVA_HOME,HADOOP_HOME,HADOOP_CONF_DIR,PATH,LANG,PYSPARK_PYTHON",
"yarn.log-aggregation-enable":"true",
"yarn.nodemanager.remote-app-log-dir":"/osp-offline/yarn-logs"})
xml("hive-site.xml",{
"hive.metastore.uris":"thrift://127.0.0.1:19083",
"hive.metastore.warehouse.dir":"/osp-offline/warehouse",
"javax.jdo.option.ConnectionURL":"jdbc:derby:;databaseName="+str(state/"metastore"/"db")+";create=true",
"javax.jdo.option.ConnectionDriverName":"org.apache.derby.jdbc.EmbeddedDriver",
"hive.metastore.schema.verification":"true",
"datanucleus.schema.autoCreateAll":"false",
"hive.server2.enable.doAs":"false"})
(conf/"env.sh").write_text(f"""export OSP_ROOT='{R}'
export JAVA_HOME='{rt}/jdk8u472-b08'
export HADOOP_HOME='{rt}/hadoop-3.3.6'
export HIVE_HOME='{rt}/apache-hive-3.1.3-bin'
export SPARK_HOME='{rt}/spark-3.5.7-bin-hadoop3'
export HADOOP_CONF_DIR='{conf}'
export YARN_CONF_DIR='{conf}'
export HIVE_CONF_DIR='{conf}'
export SPARK_CONF_DIR='{conf}'
export HADOOP_LOG_DIR='{state}/logs'
export HADOOP_PID_DIR='{state}/pids'
export HADOOP_HEAPSIZE_MAX=384
export HADOOP_HEAPSIZE=768
export YARN_HEAPSIZE=384
export HIVE_METASTORE_OPTS='-Xmx768m'
export PYSPARK_PYTHON='{rt}/python/bin/python3.11'
export PYSPARK_DRIVER_PYTHON='{rt}/python/bin/python3.11'
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:{rt}/python/bin:$PATH"
""")
(conf/"spark-defaults.conf").write_text(f"""spark.master yarn
spark.local.dir {native}/spark-local
spark.driver.memory 1g
spark.executor.memory 1g
spark.executor.cores 1
spark.executor.instances 2
spark.sql.shuffle.partitions 8
spark.sql.session.timeZone UTC
spark.sql.parquet.compression.codec snappy
spark.sql.hive.metastore.version 3.1.3
spark.sql.hive.metastore.jars path
spark.sql.hive.metastore.jars.path file://{rt}/apache-hive-3.1.3-bin/lib/*
spark.sql.warehouse.dir hdfs://127.0.0.1:19000/osp-offline/warehouse
spark.eventLog.enabled true
spark.eventLog.dir hdfs://127.0.0.1:19000/osp-offline/spark-events
spark.ui.port 18090
spark.driver.bindAddress 127.0.0.1
spark.driver.host localhost
spark.yarn.appMasterEnv.PYSPARK_PYTHON {rt}/python/bin/python3.11
spark.executorEnv.PYSPARK_PYTHON {rt}/python/bin/python3.11
""")
print("Project runtime configuration written.")
