#!/usr/bin/env bash
set -eo pipefail

# Fixed paths for the cluster submission node. Run with Bash from any directory.
if [[ "$#" != 0 ]]; then
  printf '%s\n' 'Usage: bash /home/xiong/projects/YX2602_FAULTRECOG/bin/start-heartbeat.sh' >&2
  exit 2
fi

for required_file in \
  /opt/flink-1.20.5/conf/cdh-env.sh \
  /home/xiong/projects/YX2602_FAULTRECOG/config/local.properties \
  /home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar; do
  if [[ ! -f "$required_file" || ! -r "$required_file" ]]; then
    printf 'Missing or unreadable file: %s\n' "$required_file" >&2
    exit 1
  fi
done

source /opt/flink-1.20.5/conf/cdh-env.sh
export JAVA_HOME=/opt/java11
export PATH="/opt/java11/bin:$PATH"

for required_command in /opt/java11/bin/java /opt/flink-1.20.5/bin/flink; do
  if [[ ! -x "$required_command" ]]; then
    printf 'Missing or non-executable command: %s\n' "$required_command" >&2
    exit 1
  fi
done

/opt/java11/bin/java -version
printf '%s\n' 'Submitting yx2602-heartbeat-storage to YARN. Keep the Application ID and Tracking URL printed below.'

exec /opt/flink-1.20.5/bin/flink run-application \
  -t yarn-application \
  -Denv.java.home=/opt/java11 \
  -Dcontainerized.master.env.JAVA_HOME=/opt/java11 \
  -Dcontainerized.taskmanager.env.JAVA_HOME=/opt/java11 \
  '-Denv.java.opts.all=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED' \
  -Dyarn.application.name=yx2602-heartbeat-storage \
  -Djobmanager.memory.process.size=1024m \
  -Dtaskmanager.memory.process.size=2048m \
  -Dtaskmanager.numberOfTaskSlots=2 \
  -Dclassloader.parent-first-patterns.additional=org.apache.kafka. \
  -Dyarn.ship-files=/home/xiong/projects/YX2602_FAULTRECOG/config/local.properties \
  -c com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  /home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar \
  --config local.properties --run
