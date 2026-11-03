#!/usr/bin/env bash
set -eo pipefail

source /opt/flink-1.20.5/conf/cdh-env.sh
export JAVA_HOME=/opt/java11
export PATH="/opt/java11/bin:$PATH"

# A single publisher runs on the submission node, outside YARN containers.
mode=--watch-history-config
if [[ "${1:-}" == "--once" && "$#" == 1 ]]; then
  mode=--publish-history-config
elif [[ "$#" != 0 ]]; then
  printf '%s\n' 'Usage: bash bin/watch-history-config.sh [--once]' >&2
  exit 2
fi

exec /opt/java11/bin/java \
  -Dfile.encoding=UTF-8 \
  -cp "/home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar:/opt/flink-1.20.5/lib/*:${HADOOP_CLASSPATH:-}:${HADOOP_CONF_DIR:-}" \
  com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  --config /home/xiong/projects/YX2602_FAULTRECOG/config/local.properties "$mode"
