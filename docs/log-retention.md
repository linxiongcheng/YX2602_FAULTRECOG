# 提交节点日志大小限制（30MiB，无备份）

2026-11-10按用户最新要求：提交节点 `/home/xiong/projects/YX2602_FAULTRECOG/logs` 中当前应用的心跳监控汇总日志默认上限30MiB，不保留备份；采集器状态文件默认上限10MiB，同样不保留备份。程序写入完整日志行之前检查大小，如果新行将超过上限，则清空当前文件旧内容，从新行重新开始。

## 默认保留规则

| 日志 | 大小上限 | 备份数 |
| --- | --- | --- |
| heartbeat-monitor-application_实际ID.log | 30MiB（31457280字节） | 0 |
| collector-application_实际ID.log | 10MiB（10485760字节） | 0 |

上述两类文件合计通常不超过每应用约40MiB。是单个文件增长到上限后整体清空，不是逐行裁剪以持续保留最近30MiB；清空后旧内容无法从本地备份恢复。完整保留日志行，极长单行可能超过单份上限。

升级前已经超大的当前文件，在下一次新日志写入时清空。首次写入也会删除该应用对应文件已存在的 `.1`～`.100` 纯编号备份（默认备份数为0），不删除其他应用或其他文件。多次提交作业后，多个应用各自的当前日志仍会增加总目录空间，本功能不按时间清理其他旧应用。

`heartbeat-monitor.log`继续指向最近应用的当前监控文件。`.offsets.json`和`.lock`不会作为历史日志删除，清空日志不重置事件去重位置，采集器重启也不会因此重新写入旧事件。诊断日志仍为尽力采集，进程异常退出或写入失败时不承诺完全无丢失/重复。

`collector-application_实际ID.log.bootstrap`保存JVM启动或未捕获异常输出，每次启动采集器覆盖；正常轮询状态由Java直接写入受大小限制的collector日志。

TaskManager容器内监控文件原有约256KiB＋上一份的轮转规则保留，供REST采集使用。本次不修改普通Flink/YARN/Ignite日志，不删除submit日志、检查点或业务数据。

## 配置

默认无需设置环境变量。如果之前已导出旧值，启动时需取消旧值或明确覆盖：

```bash
export HEARTBEAT_MONITOR_MAX_BYTES=31457280
export HEARTBEAT_MONITOR_BACKUP_COUNT=0
export HEARTBEAT_COLLECTOR_MAX_BYTES=10485760
export HEARTBEAT_COLLECTOR_BACKUP_COUNT=0
```

大小单位字节且必须大于0，备份数允许0～100，0表示到上限后清空当前文件；正数仍支持轮转备份，用于以后需要调整时。超过新备份数的本应用旧编号备份在首次新写入时清理。

这些设置只影响采集器，不写入Flink的local.properties，更改需重启采集器。直接调用Java时对应JVM参数依次为 `heartbeat.collector.monitor.max.bytes`、`heartbeat.collector.monitor.backup.count`、`heartbeat.collector.status.max.bytes`、`heartbeat.collector.status.backup.count`。

## 当前作业不停机更新

1. 在提交节点核对旧采集器PID及应用ID：

   ```bash
   ps -eo pid,args | grep '[c]om.hitachi.yx2602.heartbeat.HeartbeatLogCollector'
   ```

2. 用 `kill 实际采集器PID` 停止当前应用的采集器，确认进程退出。不要停止Flink/YARN进程，不删除去重或lock文件。
3. 更新 `target/heartbeat-reader-0.1.0.jar`、sha256及 `bin/collect-heartbeat-logs.sh`，保留服务器自己的local.properties。JAR和脚本需一起更新。
4. 如果之前设置过日志环境变量，按上节覆盖为新值。使用当前应用ID和实际Flink REST地址重启采集器：

   ```bash
   bash /home/xiong/projects/YX2602_FAULTRECOG/bin/collect-heartbeat-logs.sh \
     application_1773724019951_534520 http://实际Flink主机:实际REST端口
   ```

   不需要重新提交心跳作业、删除表或重启Ignite。地址取当前应用Found Web Interface或可访问的Tracking URL。
5. 持续查看日志：

   ```bash
   tail -F /home/xiong/projects/YX2602_FAULTRECOG/logs/heartbeat-monitor.log
   tail -F /home/xiong/projects/YX2602_FAULTRECOG/logs/collector-application_1773724019951_534520.log
   ```

   `tail -F`支持文件截断后继续跟踪。采集器无法启动时，检查collector日志和同名 `.bootstrap` 文件。

## 部署包

`dist/heartbeat-reader-0.1.0-java11-log-30m-no-backup-deploy.zip` 包含JAR、脚本、SQL、示例配置和文档，不包含实际local.properties。当前集群尚未应用本次修改。

本次使用Java 11定向验证采集器及独立JAR入口，15项相关测试通过。包含不留备份时的大小清空、旧编号备份清理且不影响其他文件、清空后重启去重，以及后台状态文件不生成备份；脚本通过Bash语法检查。前一版完整构建201项通过的记录保留在构建历史中，本次未重复执行全部业务测试。
