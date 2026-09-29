#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source config/runtime/env.sh
mkdir -p build/cluster/logs
# Do not reformat existing state.
if [[ ! -f build/cluster/nn/current/VERSION ]]; then
  if [[ -n "$(ls -A build/cluster/nn)" ]]; then echo "Refuse format: nonempty NameNode directory"; exit 1; fi
  hdfs namenode -format -clusterid osp-offline -nonInteractive >build/cluster/logs/format.log 2>&1
fi
# Hive 3 ships Guava 19; Hadoop 3.3.6 requires its own Guava. Preserve original.
if [[ -f "$HIVE_HOME/lib/guava-19.0.jar" ]]; then
  mkdir -p "$HIVE_HOME/compatibility-original"
  mv "$HIVE_HOME/lib/guava-19.0.jar" "$HIVE_HOME/compatibility-original/guava-19.0.jar"
  cp "$HADOOP_HOME/share/hadoop/common/lib/guava-27.0-jre.jar" "$HIVE_HOME/lib/"
fi
if [[ ! -d build/cluster/metastore/db ]]; then
  "$HIVE_HOME/bin/schematool" -dbType derby -initSchema >build/cluster/logs/hive-schema.log 2>&1
fi
hdfs namenode >build/cluster/logs/namenode.log 2>&1 &
hdfs datanode >build/cluster/logs/datanode.log 2>&1 &
yarn resourcemanager >build/cluster/logs/resourcemanager.log 2>&1 &
yarn nodemanager >build/cluster/logs/nodemanager.log 2>&1 &
"$HIVE_HOME/bin/hive" --service metastore -p 19083 --hiveconf hive.metastore.thrift.bind.host=127.0.0.1 >build/cluster/logs/metastore.log 2>&1 &
wait -n
exit 1
