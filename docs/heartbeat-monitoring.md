# 心跳链路性能日志与积压定位

2026-11-05增加周期性性能日志，用于区分读取到旧消息、解析计算耗时、向下游输出等待、入队等待，以及Ignite读取/批量写入/提交耗时。日志是客户端观测，网络、服务端锁和磁盘等待需要结合服务器监控判断。业务规则、表结构、算子UID和Flink状态结构沿用当前单表作业。

## 开启与升级

切换到纯内存窗口时，以 [内存模式部署说明](ignite-memory.md) 的新组、无Savepoint启动流程为准。下面的保存点升级流程适用于仅更新监控、需要保留Flink处理进度的场景。

更新本次生成的 `target/heartbeat-reader-0.1.0.jar`。现有JAR没有这些埋点，只改日志级别不能得到这些统计。按 [更新与效果对比](heartbeat-ignite.md#更新与效果对比) 先停止并生成Savepoint，再替换JAR并从保存点恢复，保持原消费组和其他性能参数，便于比较。

可在服务器 `/home/xiong/projects/YX2602_FAULTRECOG/config/local.properties` 添加：

```properties
monitor.log.interval.ms=30000
```

不填写时默认30000毫秒，每个阶段、每个子任务约30秒一条摘要；0关闭性能摘要及本模块的检查点事件日志，允许范围为0或1000～3600000。配置在启动时读取，修改需重新提交，不属于历史条数热更新。建议先使用30秒；短时间定位时可调为10秒。

输出同时写入容器独立监控文件和SLF4J INFO，logger为 `com.hitachi.yx2602.heartbeat.HeartbeatDiagnostics`。汇总文件不受SLF4J根日志级别影响；若还需在普通TaskManager日志中查看，而根日志级别为WARN/ERROR，请在实际使用的Log4j2配置中为该logger单独开启INFO：

```properties
logger.heartbeatPerf.name = com.hitachi.yx2602.heartbeat.HeartbeatDiagnostics
logger.heartbeatPerf.level = INFO
```

每阶段使用独立定时线程，业务线程卡住或没有新输入时仍能定期输出。统计只记录阶段、数值、任务身份和分区位点，不输出心跳原文、认证参数或密码。不逐条打印心跳；线程退出时另外输出最后一段窗口摘要。采集和格式化有少量开销，正式环境建议保持默认周期。

## 日志在哪里

### 提交节点固定目录自动汇总

更新本次JAR、`bin/start-heartbeat.sh` 和新增的 `bin/collect-heartbeat-logs.sh` 后，启动脚本自动创建提交节点目录：

```text
/home/xiong/projects/YX2602_FAULTRECOG/logs/
```

| 文件 | 内容 |
| --- | --- |
| `heartbeat-monitor.log` | 指向最近启动Application监控文件的便捷链接；已有同名普通文件时保留原文件 |
| `heartbeat-monitor-application_实际ID.log` | 各TaskManager性能摘要及检查点事件；默认30MiB上限、达到后清空旧内容、不留备份 |
| `collector-application_实际ID.log` | 汇总进程状态及错误重试；默认10MiB上限、达到后清空旧内容、不留备份 |
| `collector-application_实际ID.log.bootstrap` | JVM启动/未捕获异常输出，每次启动采集器覆盖；正常运行摘要写入上面的状态日志 |
| `submit-日期时间-进程号.log` | 本次Flink提交输出及Application ID |
| `collector-application_实际ID.pid` | 提交节点汇总进程的PID |
| `.heartbeat-monitor-application_实际ID.offsets.json` | 事件去重位置，供汇总进程重启接续 |

启动方式不变，仍可带 `--from-savepoint`。提交成功后脚本解析Application ID和Flink REST地址，用Java 11在提交节点启动一个独立后台进程（最大堆128MB），每30秒读取容器独立监控文件。不占Flink槽位，不需要Python、jq、新监听端口或YARN运行期日志聚合。提交节点必须能访问该Flink REST地址。

容器独立文件位于 `-Dlog.file` 对应的TaskManager日志目录，名为 `heartbeat-monitor.log`，约256KiB轮转并保留上一份。Flink REST日志接口需能访问该目录，标准YARN布局下它与普通TaskManager日志位于同一目录。日志携带进程唯一事件ID和递增序号，汇总时按ID去重，事件时间使用北京时间。最终目录在运行启动脚本的提交节点，不要求各YARN节点共享项目目录。

2026-11-10增加提交节点日志大小限制及旧备份自动淘汰；详见 [日志轮转与不停作业更新](log-retention.md)。默认每个Application的监控与状态日志合计约40MiB（30MiB监控＋10MiB状态，均无备份），不含bootstrap、提交输出及去重状态。不同Application的日志独立保留，不自动清理整个logs目录。文件清空不改变去重位置，不影响业务窗口或Kafka位点；持续查看应使用 `tail -F`。

首批摘要产生并被轮询后（通常约30～60秒），直接查看最近启动作业：

```bash
tail -F /home/xiong/projects/YX2602_FAULTRECOG/logs/heartbeat-monitor.log
```

查看某个指定Application时，替换实际ID：

```bash
tail -F /home/xiong/projects/YX2602_FAULTRECOG/logs/heartbeat-monitor-application_实际ID.log
```

作业已运行但尚未汇总时，无需再次提交Flink。确认运行的JAR已是本次支持容器独立文件的版本，在提交节点执行：

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/collect-heartbeat-logs.sh \
  application_实际ID http://实际Flink主机:实际REST端口
```

地址使用提交输出中的 `Found Web Interface`，不是Ignite端口。脚本不能解析地址或汇总进程启动失败时会提示，Flink提交成功的结果保持不变；不要为修复汇总进程再次提交作业。排查汇总状态：

```bash
tail -n 50 /home/xiong/projects/YX2602_FAULTRECOG/logs/collector-application_实际ID.log
```

连续10轮不能正常访问Flink REST时汇总进程退出，作业退出后通常也通过此方式停止。JobManager故障切换改变REST地址时，可用新地址重新启动汇总进程。PID文件记录的是提交节点汇总进程，不是YARN容器进程。

2026-11-05修复：先调用 `/taskmanagers/:id/logs` 获取文件列表，再按上一份、当前文件的顺序下载实际存在的监控日志。未轮转时没有 `heartbeat-monitor.log.1` 属于正常情况，不再请求该文件，避免Flink 1.20返回HTTP 500并在JobManager记录FileNotFoundException。首次摘要尚未生成时也跳过缺失文件；后续每轮重新发现文件。真正的HTTP失败仍记录并重试，不统一忽略500。列表查询与下载之间文件恰好消失的竞争仍可能导致一次失败，下轮重新发现。部署及不停作业更新方法见 [采集器修复](log-collector-fix.md)。

这是诊断日志的尽力汇总：访问失败、轮转覆盖、TaskManager提前退出或收集器长时间停机可能造成缺段；追加后保存去重位置前崩溃，也可能重放少量行。它不参与业务提交，不作为心跳账本。原始日志仍可通过Flink/YARN查看。当前收集器支持可直接访问的HTTP(S)接口，受SPNEGO等认证保护的REST需要先适配认证访问。

### 原始TaskManager与YARN日志

这些日志位于执行算子的TaskManager日志中。通过Flink Web UI进入对应TaskManager的Logs查看；YARN启用日志聚合且当前日志可获取时，也可在提交节点执行：

```bash
source /opt/flink-1.20.5/conf/cdh-env.sh
yarn logs -applicationId application_替换为当前实际ID \
  > /home/xiong/projects/YX2602_FAULTRECOG/heartbeat-perf.log

grep -E '\[HEARTBEAT-PERF\]|\[HEARTBEAT-CHECKPOINT\]' \
  /home/xiong/projects/YX2602_FAULTRECOG/heartbeat-perf.log
```

启动脚本终端不是业务监控日志流，实时查看优先使用上面的固定目录。运行中的YARN日志尚未聚合时，也可使用Flink/YARN页面查看容器日志。先核对Application、日志中的job、attempt和subtask，避免混入旧作业或已失败尝试的数据。

## 日志格式

摘要以 `[HEARTBEAT-PERF]` 开头：

```text
[HEARTBEAT-PERF] stage=ignite_sink job=... subtask=0/3 attempt=0 window_ms=30000 ...
```

以下数值仅作格式示意，实际摘要中各项按字段名排列：

```text
committed=30000 committed_per_s=1000.000
queued=1024 queue_capacity=1024 committed_total=123456 skipped_total=10
latest_read_samples=30000 latest_read_total_ms=18000.000 latest_read_avg_ms=0.600 latest_read_max_ms=25.000 latest_read_active_ms=0
bulk_write_samples=240 bulk_write_total_ms=2400.000 bulk_write_avg_ms=10.000 bulk_write_max_ms=80.000 bulk_write_active_ms=0
commit_samples=240 commit_total_ms=7200.000 commit_avg_ms=30.000 commit_max_ms=150.000 commit_active_ms=0
```

| 字段类型 | 含义 |
| --- | --- |
| `job` / `subtask=0/3` / `attempt` | 作业ID、从0开始的子任务编号/并行度、执行尝试编号 |
| `window_ms` | 本次实际采样窗口长度，不假定恰好等于配置周期 |
| `input` / `committed` 等 | 当前窗口计数，下一窗口重新统计 |
| `input_per_s` / `committed_per_s` 等 | 窗口计数除以实际窗口秒数 |
| `xxx_samples` | 本窗口已完成的该阶段调用数或年龄样本数 |
| `xxx_total_ms` / `xxx_avg_ms` / `xxx_max_ms` | 本窗口已完成调用的累计、平均、最大耗时或年龄，毫秒 |
| `xxx_active_ms` | 采样时尚未返回的该调用已持续多久；0表示没有该调用正在执行，或刚开始不足1毫秒 |
| `committed_total` / `skipped_total` | 本次写入器尝试中累计接受/跳过数 |
| `last_commit_at_ms` | 最近一次批次成功返回后的平台Unix毫秒时间，无成功批次时为0；无输入时不变正常 |
| `writer_failed` | 后台写入器已捕获异常时为1 |

计时和计数是并发窗口近似统计，不作为账本或精确审计。耗时大的操作可能跨窗口：前一窗口显示active，完成后才计入下一窗口的samples/total。阶段有包含关系，例如batch_write包含latest_read/bulk_write/commit，emit可能包含同步调用的下游阶段；buffer_wait和年龄按每条消息采样。不能将所有total相加计算CPU使用率。

## 四个阶段分别看什么

| stage | 关键字段 | 用途 |
| --- | --- | --- |
| `validation` | input、accepted、rejected、validate、emit | 原始Kafka消息速率；JSON/时间校验计算和向下游输出耗时分开 |
| `validation` | kafka_age、created_age、observed_offset_p0/p1/... | 观察Kafka时间戳/CreatedDate到校验阶段的年龄，及各分区最后观察到的原始offset |
| `split` | input、parsed、rejected、expired、empty_ports、absent_ports、split、emit | 分梯解析、错误/过期过滤、空端口统计；split只计解析计算，emit计输出等待 |
| `split` / `ordering` | since_read | 从读取校验时刻到当前阶段的时间差，可观察链路缓冲和shuffle等待 |
| `ordering` | input、accepted、duplicate、older、expired、order、emit | 单梯新旧判断和状态访问；order不含后续out.collect，emit可包含Sink阻塞 |
| `ignite_sink` | snapshot_copy、enqueue、buffer_wait、queued、queue_capacity | 快照构造、入队调用、整体缓冲等待、当前队列占用 |
| `ignite_sink` | batch_records、batches、committed、skipped、batch_write | 每批输入、成功批次、接受与跳过、store.write总耗时 |
| `ignite_sink` | transaction_start、latest_read、bulk_write、commit、history_resize | 事务开始、最新状态读取（含锁）、批量写入、提交、历史扩缩容分别计时 |
| `ignite_sink` | connect、retries、client_failures、transaction_failures、retry_backoff | 初始连接/重连、重试、客户端/事务错误和重试退避时间 |
| `ignite_sink` | created_age_before_write、created_age_after_batch | 开始写批次前、成功批次返回时输入心跳的年龄；包含数据库跳过的记录 |

读取/校验、split的input单位是原始Kafka心跳；parsed及ordering、sink的数量单位是单梯心跳。一条原始消息可拆为最多三台梯，部分还会被过滤，不能把各阶段计数简单一比一比较。

`buffer_wait` 从建立入队快照开始，直到该记录所在批次准备调用store.write，包含快照构造、入队阻塞、队列和凑批等待；`enqueue`单独测量入队调用。`batch_write`包含store.write内部排序、事务、重试及重连，不包含前面的排队/凑批。`history_resize`可能包含额外的getAll/removeAll/putAll。客户端调用耗时包含网络往返、服务端处理和可能的锁等待，不能直接等同于磁盘耗时。

消息年龄按平台时钟减消息时间计算，负值在摘要中按0统计，需保持节点时钟同步。Kafka时间戳可能是生产者CreateTime或Broker的LogAppendTime，其语义取决于配置。CreatedDate还是业务时间；这些统计不改变过滤规则。

## 按证据判断积压来源

| 日志与外部监控现象 | 优先排查 |
| --- | --- |
| validate或split的total长期占据大部分窗口时间，emit较低，CPU也高 | 校验/解析计算、消息体大小或字段复杂度 |
| split计算较快，emit很大；ordering的emit、sink的enqueue也很大，queued接近容量 | 下游写入等待传导到解析，不能把这部分认作纯解析慢 |
| order较慢，Sink队列较低 | keyed state访问、GC及该TaskManager资源 |
| snapshot_copy较慢，enqueue较低 | 入库快照字段复制、对象分配和GC |
| latest_read累计耗时占主要部分，或active持续增长 | 逐梯读取网络往返、同梯事务锁、服务端响应；结合网络和Ignite监控定位 |
| bulk_write慢或长期active | 批量更新、服务端事务处理、锁、网络响应；结合磁盘及服务端线程检查 |
| commit慢，且数据/WAL盘写延迟高 | 优先核对持久化提交、WAL和磁盘竞争；commit耗时本身不能证明一定是磁盘 |
| connect/retries/client_failures升高 | 网络、服务可用性、连接与请求超时；检查原始异常日志 |
| history_resize明显增多 | 最近是否动态调整保留条数，是否大量电梯正在应用新上限 |
| Kafka提交LAG很大，但观察offset持续前进、年龄较低、Checkpoint持续成功 | 核对实际读取位置与已提交位点，区分提交周期与真实处理积压 |
| kafka_age/created_age在validation阶段已经很高 | 输入已经较旧，可能来自上游发送延迟或Kafka读取落后，需要结合分区LAG |
| expired增多、committed减少，Kafka LAG却下降 | 可能在过滤过期消息，不能把LAG下降都归为有效入库提升 |

对比时使用同一个作业、同一attempt、相近时间窗口及全部子任务，避免只看max或把不同单位相减。没有输入时计数为0但仍有摘要，能区分空闲与日志缺失。

## 检查点与提交LAG

`[HEARTBEAT-CHECKPOINT]` 记录checkpoint_id及如下事件：

- `flush_started`：Sink开始等待检查点前的数据提交。
- `flush_finished`：Sink队列屏障之前的数据已提交，snapshotState可以返回。
- `completed`：收到Flink的检查点全局完成通知。
- `aborted`：收到检查点中止通知。

性能摘要中的 `checkpoint_flush` 提供对应等待耗时和active；`checkpoint_flush_ok/failed` 提供窗口结果计数。completed通知不等同于Kafka位点提交请求已被Broker确认，需要对照KafkaSource已有的currentOffset/committedOffset指标或消费组查询。

`observed_offset_pN` 是校验阶段最后观察到的原始消息offset，不是Kafka末尾，不是提交位置，也不证明该消息已经写入Ignite。Kafka的CURRENT-OFFSET通常表示下一条位置，与原始消息offset有1的计数差异。摘要不额外轮询Broker，不单独声明真实LAG；需要与KafkaSource和Broker同一分区的位点比较。

若客户端日志定位到写入等待，继续在Ignite宿主机关联同一时间段的 `pidstat -d`、`iostat -x`、容器CPU/内存，以及服务端日志。现有数据目录和磁盘记录见 [Ignite实际持久化目录](heartbeat-ignite.md#ignite实际持久化目录)。

## 本地验证

2026-11-05使用Java 11执行离线 `mvn -o -B -ntp clean verify`，171项测试通过，无失败、错误或跳过。新增测试验证窗口速率与重置、计算和emit计时分离、跨窗口未完成调用、关闭监控、日志输出异常隔离、配置范围，以及真实Ignite批量调用被阻塞时的阶段识别。另验证写入线程和入队线程同时阻塞时，独立定时摘要仍然输出。最终JAR的独立写库和配置发布入口均通过验证。

本地测试不等同于集群吞吐测量或生产根因确认；上线后按同一时间段的阶段摘要、Flink指标和服务端I/O监控交叉定位。

固定目录汇总版本同日在Java 11下重新执行离线 `mvn -o -B -ntp clean verify`，177项测试全部通过。新增验证包括真实Flink 1.20.5 REST读取独立日志及轮转文件、多TaskManager汇总、重复轮询和进程重启去重、不完整日志行，以及不带Flink库的最终打包收集器运行。两个脚本通过Bash语法与LF检查；提交成功/失败、保存点参数、REST地址发现与回退、收集器失败隔离共6种模拟场景通过验证。实际集群的REST连通性与认证仍需部署时确认。
