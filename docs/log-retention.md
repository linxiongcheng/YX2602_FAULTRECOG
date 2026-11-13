# 项目日志按天滚动，保留最近7天

2026-11-13起，替代旧版“达到30MiB后清空”策略：监控汇总达到30MiB后分段续写，跨北京时间午夜也会分段。保留当天及前6个自然日，不限制为3份备份；只有超过7天期限的日志才清理。单个文件30MiB不等于整个目录上限30MiB，7天内日志总量可以更大。

## 文件与分段

| 文件 | 行为 |
| --- | --- |
| heartbeat-monitor-application_实际ID.log | 当前监控汇总文件，默认30MiB分段 |
| heartbeat-monitor-application_实际ID.log.2026-11-13.000001 | 该日第一份已封存段；继续产生000002、000003，不覆盖当天旧段 |
| collector-application_实际ID.log | 当前采集器状态，默认10MiB分段，同样按天保留7天 |
| collector-application_实际ID.log.2026-11-13.000001 | 状态日志已封存段 |
| collector-application_实际ID.log.bootstrap.2026-11-13.进程号 | 本次采集器启动及未捕获异常输出，每次启动独立保存 |
| collector-application_实际ID.log.bootstrap | 指向最近一次bootstrap输出的便捷链接 |
| heartbeat-monitor.log | 指向最近启动应用的当前监控文件；已有同名普通文件仍保留 |
| submit-日期时间-进程号.log | 作业提交输出，按文件最后修改日期保留7天 |

当前文件保留原名字，方便 `tail -F`。新行会超过大小上限时，将原文件整体改名为带日期、分段号的文件，再创建当前文件写入新行。跨天即使没有新心跳，运行中的采集器也会在轮询时封存前一天文件。每行完整保留；单条异常长日志可能超过大小上限。升级前已经超大的文件也整体封存，不丢弃其内容，故既有超大段可能超过30MiB。

旧的 `.log.1` 等编号备份及旧bootstrap文件不会立即删除；过期后按最后修改日期清理。旧bootstrap普通文件在首次新版脚本启动时改名保留为 `.bootstrap.legacy.时间戳.进程号`，后续按日期使用独立文件。

## 清理范围与执行时机

按Asia/Shanghai计算保留日期。例如11月13日保留11月7日至13日，清理11月6日及更早日期。日期段按文件名中的封存日期判断；旧版无日期日志和submit按最后修改日期判断，不解析日志行的业务CreatedDate。

采集器启动后、每小时，以及跨天后的下一次轮询，检查日志目录中所有Application的过期项目日志，不只处理当前应用。仅匹配本项目固定命名的monitor、collector、bootstrap、submit文件，不递归删除目录、不跟随符号链接，不处理其他名称的日志。多个采集器用同一目录清理锁避免互相清理冲突。

旧当前文件及bootstrap在删除前尝试获取所属采集器锁；仍在运行的采集器拥有该锁时，保留这些文件以免删除正在写入的内容。日期封存段不再追加，可正常过期清理。运行中的采集器自身会主动封存跨天的monitor和status文件；正在运行进程持有的bootstrap可能推迟到该进程退出后清理。

`.pid`、`.offsets.json`、`.offsets.json.next`、`.lock`等运行与去重文件均不清理。去重位置独立于日志文件是否存在，7天后日志删除、采集器重启均不会因此将旧事件重新灌入。

TaskManager容器里的约256KiB＋上一份轮转规则保留，供REST抓取；本功能不改变普通Flink/YARN容器日志、Ignite服务日志、业务数据和HDFS检查点。清理失败记录cleanup_error，正常日志采集继续尝试。

## 配置

新版同步脚本默认使用以下值，无需写入local.properties：

```bash
export HEARTBEAT_MONITOR_MAX_BYTES=31457280
export HEARTBEAT_COLLECTOR_MAX_BYTES=10485760
export HEARTBEAT_LOG_RETENTION_DAYS=7
```

大小是字节且需大于0，保留天数范围1～3650。修改后重启采集器生效；不影响运行中的Flink业务作业。旧的HEARTBEAT_MONITOR_BACKUP_COUNT、HEARTBEAT_COLLECTOR_BACKUP_COUNT及对应backup.count JVM参数已不再使用，不能限制日内段数或触发清空。

Java入口参数对应 `heartbeat.collector.monitor.max.bytes`、`heartbeat.collector.status.max.bytes`、`heartbeat.collector.retention.days`。

## 当前作业不停机更新

1. 记录现有采集器的应用ID、REST地址和PID：

   ```bash
   ps -eo pid,args | grep '[c]om.hitachi.yx2602.heartbeat.HeartbeatLogCollector'
   ```

2. 用 `kill 实际采集器PID` 停止需要更新的采集器，确认进程退出；保留日志、去重及锁文件。不要停止Flink/YARN任务。
3. 在内网更新源码及三个脚本： `bin/collect-heartbeat-logs.sh`、`bin/cleanup-heartbeat-logs.sh`、`bin/start-heartbeat.sh`。使用Java 11重新构建JAR，保留服务器local.properties：

   ```bash
   export JAVA_HOME=/opt/java11
   export PATH=/opt/java11/bin:$PATH
   cd /home/xiong/projects/YX2602_FAULTRECOG
   mvn -DskipTests clean package
   ```

4. 使用原应用ID和REST地址重新运行采集脚本：

   ```bash
   bash /home/xiong/projects/YX2602_FAULTRECOG/bin/collect-heartbeat-logs.sh \
     application_实际ID http://实际Flink主机:实际REST端口
   ```

   不需要重新提交业务作业，也不需要重启Ignite。
5. 继续查看当前日志：

   ```bash
   tail -F /home/xiong/projects/YX2602_FAULTRECOG/logs/heartbeat-monitor.log
   tail -F /home/xiong/projects/YX2602_FAULTRECOG/logs/collector-application_实际ID.log
   ```

   历史段从日期较早、编号较小到较大查看；最近完整日志可能跨多个段文件。

## 没有采集器运行时也要清理

采集器全部停止后没有后台进程执行按时清理。新增独立脚本不需要访问Flink REST、Kafka或Ignite，可以随时执行：

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/cleanup-heartbeat-logs.sh
```

输出removed_log_files=N。若希望不论作业是否运行都定时清理，可由用户在提交节点的 `crontab -e` 中追加（不要覆盖已有任务）：

```cron
15 * * * * /bin/bash /home/xiong/projects/YX2602_FAULTRECOG/bin/cleanup-heartbeat-logs.sh
```

该定时任务不会自动安装；上线时按需要配置。同目录的采集器清理和独立脚本会用锁协调。直接使用Java时可执行 `HeartbeatLogCollector --cleanup-logs --output-dir 目录` 并通过retention.days参数设置期限。

## 验证

2026-11-13在Java 11下执行全量 `mvn -o -B -ntp clean verify`，206项测试通过，无失败、错误或跳过。覆盖中文UTF-8大小边界、保留完整行、多个段不覆盖、重启编号接续、北京时间午夜及无输入时封存、7天自然日边界、跨Application旧日志、运行中采集器锁保护、无关文件和运行状态保护，以及日志删除后去重接续。

最终入口提示调整后，另执行18项日志模块及独立JAR采集入口定向验证，全部通过。独立Java清理入口在临时目录验证了只删除7天前的日志、保留边界日及去重文件；三个脚本均通过Bash语法和非法参数检查。未生成或更新部署包，未操作内网服务或安装系统定时任务。
