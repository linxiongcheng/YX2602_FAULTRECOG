#!/usr/bin/env bash
set -eo pipefail

# Read-only statistics from the collector's local log; no Flink, Kafka or Ignite connection.
exec /opt/java11/bin/java -Xms32m -Xmx256m -Dfile.encoding=UTF-8 \
  -cp /home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar \
  com.hitachi.yx2602.heartbeat.HeartbeatPerformanceReport "$@"
