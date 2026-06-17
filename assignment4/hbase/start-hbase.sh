#!/bin/bash
set -euo pipefail

mkdir -p /opt/hbase-data/rootdir /opt/hbase-data/logs
mkdir -p /opt/hbase-data/zookeeper

#region debug-point hbase-bootstrap-state
echo "[debug] hostname=$(hostname)"
echo "[debug] HBASE_HOME=${HBASE_HOME}"
echo "[debug] rootdir_contents_before_start"
find /opt/hbase-data/rootdir -maxdepth 2 -mindepth 1 | sort || true
echo "[debug] wal_dirs_before_start"
find /opt/hbase-data/rootdir/WALs -maxdepth 1 -mindepth 1 2>/dev/null | sort || true
#endregion

${HBASE_HOME}/bin/hbase-daemon.sh start zookeeper
${HBASE_HOME}/bin/hbase-daemon.sh start master
${HBASE_HOME}/bin/hbase-daemon.sh start regionserver

deadline=$((SECONDS + 360))
while [ $SECONDS -lt $deadline ]; do
  # Lightweight check: bash built-in TCP test = master+ZK are up
  if timeout 3 bash -c "echo >/dev/tcp/localhost/16010" 2>/dev/null; then
    echo "[check] Master port 16010 is listening"
    break
  fi
  echo "[check] Waiting for HBase master... (${SECONDS}s elapsed)"
  sleep 5
done

if [ $SECONDS -ge $deadline ]; then
  echo "HBase core services did not become ready in time" >&2
  echo "=== Master log ===" >&2
  cat ${HBASE_HOME}/logs/hbase-*-master-*.log 2>/dev/null || true
  echo "=== Master out ===" >&2
  cat ${HBASE_HOME}/logs/hbase-*-master-*.out 2>/dev/null || true
  echo "=== RegionServer log ===" >&2
  cat ${HBASE_HOME}/logs/hbase-*-regionserver-*.log 2>/dev/null || true
  echo "=== RegionServer out ===" >&2
  cat ${HBASE_HOME}/logs/hbase-*-regionserver-*.out 2>/dev/null || true
  echo "=== Zookeeper out ===" >&2
  cat ${HBASE_HOME}/logs/hbase-*-zookeeper-*.out 2>/dev/null || true
  ps aux 2>/dev/null || true
  exit 1
fi

${HBASE_HOME}/bin/hbase thrift start -p 9090 > /opt/hbase-data/logs/thrift.log 2>&1 &
${HBASE_HOME}/bin/hbase rest start -p 8080 --infoport 8085 > /opt/hbase-data/logs/rest.log 2>&1 &

echo "HBase services started"
tail -F ${HBASE_HOME}/logs/* /opt/hbase-data/logs/*.log
