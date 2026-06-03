#!/bin/bash
set -e

sudo service ssh start

sudo mkdir -p /tmp/hadoop-hduser/dfs/name /tmp/hadoop-hduser/dfs/data
sudo chown -R hduser:hduser /tmp/hadoop-hduser

if [ ! -f "/tmp/hadoop-hduser/dfs/name/current/VERSION" ]; then
  "$HADOOP_HOME/bin/hdfs" namenode -format
fi

"$HADOOP_HOME/sbin/start-dfs.sh"

if [[ -z "${YARNSTART:-}" || "${YARNSTART}" -ne 0 ]]; then
  "$HADOOP_HOME/sbin/start-yarn.sh"
fi

"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /tmp
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /users
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /jars
"$HADOOP_HOME/bin/hdfs" dfs -chmod 777 /tmp
"$HADOOP_HOME/bin/hdfs" dfs -chmod 777 /users
"$HADOOP_HOME/bin/hdfs" dfs -chmod 777 /jars
"$HADOOP_HOME/bin/hdfs" dfsadmin -safemode leave || true

tail -f "$HADOOP_HOME"/logs/hadoop-*-namenode-*.log
