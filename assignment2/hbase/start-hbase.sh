#!/bin/bash
set -euo pipefail

mkdir -p /opt/hbase-data/rootdir /opt/hbase-data/logs /opt/hbase-data/zookeeper

${HBASE_HOME}/bin/hbase-daemon.sh start zookeeper
${HBASE_HOME}/bin/hbase-daemon.sh start master
${HBASE_HOME}/bin/hbase-daemon.sh start regionserver

deadline=$((SECONDS + 180))
while [ $SECONDS -lt $deadline ]; do
  if timeout 10 bash -lc "echo list | ${HBASE_HOME}/bin/hbase shell -n >/dev/null 2>&1"; then
    break
  fi
  sleep 2
done

if [ $SECONDS -ge $deadline ]; then
  echo "HBase services did not become ready in time" >&2
  exit 1
fi

${HBASE_HOME}/bin/hbase thrift start -p 9090 > /opt/hbase-data/logs/thrift.log 2>&1 &
echo "HBase services started"

tail -F ${HBASE_HOME}/logs/* /opt/hbase-data/logs/*.log
