#!/bin/bash
set -euo pipefail

if [[ -z "${FLUME_CONF:-}" ]]; then
  echo "FLUME_CONF is required"
  exit 1
fi

CONF_FILE="/opt/project/conf/${FLUME_CONF}"
if [[ ! -f "${CONF_FILE}" ]]; then
  echo "Cannot find Flume config: ${CONF_FILE}"
  exit 1
fi

exec ${FLUME_HOME}/bin/flume-ng agent \
  --name a1 \
  --conf ${FLUME_HOME}/conf \
  --conf-file ${CONF_FILE} \
  -Dflume.root.logger=INFO,console
