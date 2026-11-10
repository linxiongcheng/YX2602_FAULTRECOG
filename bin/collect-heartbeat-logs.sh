#!/usr/bin/env bash
set -eo pipefail

if [[ "$#" != 2 || ! "$1" =~ ^application_[0-9]+_[0-9]+$ || ! "$2" =~ ^https?:// ]]; then
  printf '%s\n' 'Usage: bash bin/collect-heartbeat-logs.sh APPLICATION_ID FLINK_REST_URL' >&2
  exit 2
fi

application_id="$1"
rest_url="$2"
log_dir=/home/xiong/projects/YX2602_FAULTRECOG/logs
pid_file="$log_dir/collector-$application_id.pid"
collector_log="$log_dir/collector-$application_id.log"
mkdir -p "$log_dir"

if [[ -r "$pid_file" ]]; then
  read -r collector_pid < "$pid_file" || true
  if [[ "${collector_pid:-}" =~ ^[0-9]+$ ]] && kill -0 "$collector_pid" 2>/dev/null; then
    collector_args=$(tr '\0' ' ' < "/proc/$collector_pid/cmdline" 2>/dev/null || true)
    if [[ "$collector_args" == *com.hitachi.yx2602.heartbeat.HeartbeatLogCollector* && "$collector_args" == *"$application_id"* ]]; then
      printf 'Collector already running: PID=%s\n' "$collector_pid"
      exit 0
    fi
  fi
fi

nohup /opt/java11/bin/java -Xms32m -Xmx128m -Dfile.encoding=UTF-8 \
  "-Dheartbeat.collector.monitor.max.bytes=${HEARTBEAT_MONITOR_MAX_BYTES:-31457280}" \
  "-Dheartbeat.collector.monitor.backup.count=${HEARTBEAT_MONITOR_BACKUP_COUNT:-0}" \
  "-Dheartbeat.collector.status.max.bytes=${HEARTBEAT_COLLECTOR_MAX_BYTES:-10485760}" \
  "-Dheartbeat.collector.status.backup.count=${HEARTBEAT_COLLECTOR_BACKUP_COUNT:-0}" \
  -cp /home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar \
  com.hitachi.yx2602.heartbeat.HeartbeatLogCollector \
  --rest-url "$rest_url" --application-id "$application_id" --output-dir "$log_dir" \
  > "$collector_log.bootstrap" 2>&1 < /dev/null &
collector_pid=$!
printf '%s\n' "$collector_pid" > "$pid_file"
sleep 1
if ! kill -0 "$collector_pid" 2>/dev/null; then
  printf 'Collector exited; inspect %s and %s.bootstrap\n' "$collector_log" "$collector_log" >&2
  exit 1
fi
latest_log="$log_dir/heartbeat-monitor.log"
if [[ ! -e "$latest_log" || -L "$latest_log" ]]; then
  if ! ln -sfn "heartbeat-monitor-$application_id.log" "$latest_log"; then
    printf 'Could not update latest-log link: %s\n' "$latest_log" >&2
  fi
fi
printf 'Monitor log: %s/heartbeat-monitor-%s.log\n' "$log_dir" "$application_id"
printf 'Collector status: %s (PID=%s)\n' "$collector_log" "$collector_pid"
