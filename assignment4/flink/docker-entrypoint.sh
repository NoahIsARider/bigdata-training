#!/bin/bash
set -euo pipefail

FLINK_CONF_FILE="${FLINK_HOME}/conf/flink-conf.yaml"

append_conf_if_missing() {
  local line="$1"
  if ! grep -qxF "${line}" "${FLINK_CONF_FILE}"; then
    echo "${line}" >> "${FLINK_CONF_FILE}"
  fi
}

append_conf_if_missing "jobmanager.bind-host: 0.0.0.0"
append_conf_if_missing "rest.bind-address: 0.0.0.0"
append_conf_if_missing "taskmanager.bind-host: 0.0.0.0"

if [[ -n "${FLINK_PROPERTIES:-}" ]]; then
  while IFS= read -r line; do
    if [[ -z "${line// }" ]]; then
      continue
    fi
    append_conf_if_missing "${line}"
  done <<< "${FLINK_PROPERTIES}"
fi

case "${1:-help}" in
  jobmanager)
    exec "${FLINK_HOME}/bin/jobmanager.sh" start-foreground
    ;;
  taskmanager)
    exec "${FLINK_HOME}/bin/taskmanager.sh" start-foreground
    ;;
  help|--help|-h)
    exec "${FLINK_HOME}/bin/flink" --help
    ;;
  *)
    exec "$@"
    ;;
esac
