#!/usr/bin/env bash
set -eo pipefail

# Fixed paths for the cluster submission node. Run with Bash from any directory.
restore_args=()
job_name_args=()
application_name=yx2602-heartbeat-storage
usage() {
  printf '%s\n' 'Usage: bash /home/xiong/projects/YX2602_FAULTRECOG/bin/start-heartbeat.sh [--job-name NAME] [--from-savepoint PATH]' >&2
}
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --job-name)
      if [[ "$#" -lt 2 || "${#job_name_args[@]}" != 0 || -z "${2//[[:space:]]/}" || "$2" == -* \
            || "$2" =~ ^[[:space:]] || "$2" =~ [[:space:]]$ || "$2" == *$'\n'* || "$2" == *$'\r'* ]]; then
        usage
        exit 2
      fi
      application_name="$2"
      job_name_args=(--job-name "$2")
      shift 2
      ;;
    --from-savepoint|-s)
      if [[ "$#" -lt 2 || "${#restore_args[@]}" != 0 || -z "$2" || "$2" == -* ]]; then
        usage
        exit 2
      fi
      restore_args=(-s "$2")
      shift 2
      ;;
    *) usage; exit 2 ;;
  esac
done

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
log_dir=/home/xiong/projects/YX2602_FAULTRECOG/logs
mkdir -p "$log_dir"
submission_log="$log_dir/submit-$(TZ=Asia/Shanghai date +%Y%m%d-%H%M%S)-$$.log"
printf 'Submitting %s to YARN. Keep the Application ID and Tracking URL printed below.\n' "$application_name"
if [[ "${#restore_args[@]}" != 0 ]]; then
  printf 'Restoring Flink state and Kafka positions from: %s\n' "${restore_args[1]}"
fi

set +e
/opt/flink-1.20.5/bin/flink run-application \
  -t yarn-application \
  -Denv.java.home=/opt/java11 \
  -Dcontainerized.master.env.JAVA_HOME=/opt/java11 \
  -Dcontainerized.taskmanager.env.JAVA_HOME=/opt/java11 \
  '-Denv.java.opts.all=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED' \
  "-Dyarn.application.name=$application_name" \
  -Djobmanager.memory.process.size=1024m \
  -Dtaskmanager.memory.process.size=2048m \
  -Dtaskmanager.numberOfTaskSlots=2 \
  -Dclassloader.parent-first-patterns.additional=org.apache.kafka. \
  -Dyarn.ship-files=/home/xiong/projects/YX2602_FAULTRECOG/config/local.properties \
  -c com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  "${restore_args[@]}" \
  /home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar \
  --config local.properties --run "${job_name_args[@]}" 2>&1 | tee "$submission_log"
submission_status=("${PIPESTATUS[@]}")
set -e
if [[ "${submission_status[0]}" != 0 ]]; then
  exit "${submission_status[0]}"
fi
if [[ "${submission_status[1]}" != 0 ]]; then
  printf 'Submission succeeded, but saving its log failed: %s\n' "$submission_log" >&2
  exit 0
fi

application_id=$(grep -oE 'application_[0-9]+_[0-9]+' "$submission_log" | tail -n 1 || true)
rest_url=$(sed -nE 's/.*Found Web Interface ([^ ]+).*/\1/p' "$submission_log" | tail -n 1)
if [[ -n "$rest_url" && "$rest_url" != http://* && "$rest_url" != https://* ]]; then
  rest_url="http://$rest_url"
fi
if [[ -n "$application_id" && -z "$rest_url" ]]; then
  rest_url=$(yarn application -status "$application_id" 2>/dev/null \
    | sed -nE 's/^[[:space:]]*Tracking-URL[[:space:]]*:[[:space:]]*//p' | head -n 1 || true)
fi
collector_script=/home/xiong/projects/YX2602_FAULTRECOG/bin/collect-heartbeat-logs.sh
if [[ -n "$application_id" && -n "$rest_url" && -r "$collector_script" ]]; then
  if ! bash "$collector_script" "$application_id" "$rest_url"; then
    printf '%s\n' 'The Flink application was submitted. Log collection failed; do not resubmit the job to fix the collector.' >&2
  fi
else
  printf 'The Flink application was submitted. Start collect-heartbeat-logs.sh manually using its Application ID and REST URL. Submission log: %s\n' "$submission_log" >&2
fi
