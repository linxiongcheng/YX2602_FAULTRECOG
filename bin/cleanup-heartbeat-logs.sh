#!/usr/bin/env bash
set -eo pipefail

if [[ "$#" != 0 ]]; then
  printf '%s\n' 'Usage: bash /home/xiong/projects/YX2602_FAULTRECOG/bin/cleanup-heartbeat-logs.sh' >&2
  exit 2
fi

exec /opt/java11/bin/java -Xms16m -Xmx64m \
  "-Dheartbeat.collector.retention.days=${HEARTBEAT_LOG_RETENTION_DAYS:-7}" \
  -cp /home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar \
  com.hitachi.yx2602.heartbeat.HeartbeatLogCollector \
  --cleanup-logs --output-dir /home/xiong/projects/YX2602_FAULTRECOG/logs
