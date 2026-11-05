# YX2602 心跳数据读取、解析与存储

Java 11 / Flink 1.20.5 / Ignite 2.18.0 实现。读取Kafka心跳，完成格式校验、分梯、字段规范化、10分钟过期过滤及按电梯判断新旧，输出带历史序号的单梯状态。读取和解析异常都可靠写入配置的异常Topic。解析规则与字段映射详见 [解析模块说明](docs/heartbeat-parser.md)。正式写入与部署详见 [Ignite 模块说明](docs/heartbeat-ignite.md)。

正式上线从 [部署执行顺序](docs/heartbeat-ignite.md#正式部署顺序) 开始，包含已有Ignite服务接入、Java 11的YARN容器参数、动态配置同步、提交与验收、Checkpoint/Savepoint目录及停止恢复命令，以及AM启动失败排查。

旧表改为查询无需引号的结构，按 [清空重建与重新部署](docs/redeploy-unquoted-columns.md) 执行。

查找服务器数据位置：见 [Ignite实际持久化目录](docs/heartbeat-ignite.md#ignite实际持久化目录)。已核对 `AI-slave06` 的宿主机数据目录为 `/home/workdir/ignite2/standalone/ignite-data/db`，对应容器 `/opt/ignite/work/db`，位于挂载到 `/home` 的 `/dev/sdb1`（XFS）。文档包含心跳表完整目录、核查依据及查看命令。

保留消费积压并将并行度从2调整为3：按 [保留积压调整并行度](docs/heartbeat-ignite.md#保留积压调整并行度) 先停止并生成Savepoint，再用启动脚本的 `--from-savepoint` 参数恢复；无需重新打包JAR。

2026-11-05写入优化：同批每梯只读取一次最新状态，按原顺序计算最新/历史槽位后使用一次 `putAll` 写入，减少客户端网络调用。新增批次大小、耗时与最后成功返回时间指标；升级JAR与对比方法见 [写入吞吐优化与验收](docs/heartbeat-ignite.md#写入吞吐优化与验收)。

定位积压：新版默认每30秒输出读取校验、分梯解析、新旧判断、Ignite写入的性能摘要，分别统计纯计算、输出等待、入队和读写/提交耗时；配置与日志判读见 [性能日志与积压定位](docs/heartbeat-monitoring.md)。

新版启动脚本自动将各TaskManager监控日志汇总到提交节点 `/home/xiong/projects/YX2602_FAULTRECOG/logs/heartbeat-monitor-<ApplicationID>.log`。需同步更新JAR和两个脚本，通过Flink现有REST接口读取，不依赖YARN运行期日志聚合。

## 实现范围

```text
Kafka 原始心跳（bytes，保留来源坐标）
  → KafkaSource<RawHeartbeat>
  → HeartbeatReader.ValidationFunction
      ├─ DataStream<HeartbeatMessage> → HeartbeatParser.SplitFunction（分梯与字段校验）
      │   ├─ keyBy(EId) → HeartbeatParser.OrderingFunction → DataStream<ElevatorUpdate> → Ignite事务写入（--run）
      │   └─ 解析异常 → KafkaSink<String> → 同一个异常Topic
      └─ 异常侧输出 → KafkaSink<String> → 异常 Topic
```

读取层保留原始JSON，将CreatedDate按Asia/Shanghai严格解析为业务时间；解析层对最多三台电梯独立处理，规范化文档定义的业务字段，并维护每台电梯的最后接受时间和历史序号。`--run` 将最新状态和最近N条历史事务写入 Ignite 2.18.0，N默认10，支持共享HDFS配置热更新；故障真实性判别尚未实现。`--preview` 已接上完整读取与解析链路，输出带全部已定义字段（含null）的单梯JSON。

| 约定 | 实现 |
| --- | --- |
| 格式 | UTF-8 JSON 对象，禁止重复键、尾随第二个 JSON、非标准 NaN/Infinity；嵌套深度最多100层 |
| CreatedDate | 必填北京时间字符串，格式yyyy/M/d H:mm:ss，可选1～3位毫秒小数；统一转为UTC Unix毫秒 |
| SendTime | 可选来源字段；为空或值非法不拒收，不参与排序、过期和未来校验 |
| 未来时间 | 默认最多超前平台时间60000毫秒，恰好60000允许，阈值可配置 |
| 历史时间 | 读取层放行合法历史时间；解析层过滤延迟超过10分钟的数据，恰好10分钟允许 |
| 单梯新旧 | 只接受比该梯最后接受时间更晚的记录；同时间或更早的数据过滤并计数 |
| 序号与状态 | 每梯序号从1开始，仅接受新记录时递增；状态无TTL，检查点恢复后延续 |
| 空消息 | Kafka tombstone、空字节和非法编码都进入异常 Topic |
| 首次启动 | 没有消费位点的分区从初始化时的最新位置开始 |
| 后续启动 | 保留消费组已提交位点；通过 `-s` 恢复时以检查点/保存点状态为准 |
| 异常交付 | 至少一次，允许重放产生重复消息；失败重试，持续失败触发任务恢复 |

**业务时间只取CreatedDate，时区固定为北京时间（Asia/Shanghai）。** 例如 `2026/1/22 17:56:17` 对应UTC `2026-01-22T09:56:17Z`，不受JVM默认时区影响。缺失、空值或非法日期写异常Topic，不回退到SendTime、Kafka时间或接收时间。同梯CreatedDate相同仍按重复过滤；上游只提供秒精度时，同一秒最多接受一条。平台时钟用于超前与过期判断，需要节点时钟同步。

按新项目首次部署：执行当前建表脚本创建 `PUBLIC.HB_STATE`，不迁移旧数据、不加载旧项目的Checkpoint/Savepoint。时间规则详见 [CreatedDate说明](docs/heartbeat-time.md)。

存储使用一张 `HB_STATE` 表：主键为 `(EID, SLOT)`，`SLOT=-1` 是最新状态，`SLOT>=0` 是最近N条历史，每梯最多N+1行。业务列沿用心跳文档中拆分后的41个字段拼写，建表不加字段双引号，Ignite统一保存为大写；查询直接写 `eid`、`floor`、`mainfaultcode`、`createddate` 即可。`part_day` 按CreatedDate的北京时间日期生成。查历史须过滤掉SLOT=-1。完整查询示例见 [存储说明](docs/heartbeat-ignite.md#检查点重试与运维)。

## 工程结构

生产代码共18个Java文件。阅读主流程时，依次查看 `HeartbeatReaderJob → HeartbeatReader → HeartbeatParser → IgniteHeartbeatSink`。各模块内部辅助逻辑放在所属文件中，跨模块使用的数据模型保持独立。

```text
src/main/java/com/hitachi/yx2602/heartbeat/
  HeartbeatReaderJob.java       配置检查、数据库预检、预览及正式运行入口
  ReaderConfig.java             配置加载、环境变量替换、读取与解析参数校验
  IgniteConfig.java             Ignite连接、队列、事务、重试配置及历史策略热加载
  HeartbeatReader.java          KafkaSource、首次位点初始化、反序列化、校验算子、异常Sink
  HeartbeatValidator.java       可独立测试的UTF-8、JSON与业务时间校验
  HeartbeatTime.java            CreatedDate北京时间解析与SendTime可选元数据
  HeartbeatParser.java          分梯算子、过期过滤、keyed state新旧判断、解析异常Sink
  HeartbeatSplitter.java        单梯字段解析、规范化、按端口生成异常
  IgniteHeartbeatSink.java      Flink写入算子、检查点与正常结束刷新
  AsyncHeartbeatWriter.java     有界异步队列、批处理、提交屏障及内部存储接口
  IgniteHeartbeatStore.java     表结构校验、不可变写入快照、最新/历史事务与重试
  HeartbeatDiagnostics.java     独立定时摘要、阶段计时、吞吐与积压定位日志
  HeartbeatLogCollector.java    提交节点通过Flink REST汇总容器监控日志
  RawHeartbeat.java             Kafka原始字节和来源坐标
  HeartbeatMessage.java         校验后未分梯的心跳
  ElevatorState.java            可空业务字段
  ElevatorUpdate.java           单梯状态、时间、序号和来源信息
  CreatedVersion.java           Flink保存的每梯业务时间和历史序号
sql/ignite2-heartbeat.sql        手工建表脚本
config/reader.example.properties
bin/watch-history-config.sh     提交节点配置同步入口；共享策略文件由程序自动生成
bin/start-heartbeat.sh          使用固定集群路径提交YARN Application正式任务
src/test/java/                  字段、时间、状态恢复、事务、背压、真实Flink/Ignite及打包测试
```

`HeartbeatReader` 内的 `Deserializer` 和 `ValidationFunction` 负责Flink接入；`HeartbeatParser` 内的 `SplitFunction` 和 `OrderingFunction` 负责流式处理。`IgniteHeartbeatStore` 内的 `Schema`、`Row` 分别管理表结构和写入快照。`AsyncHeartbeatWriter.Store` 是异步写入器内部的存储接口，测试通过它模拟阻塞和失败。

字段解析、事务写入和异步队列各自包含独立的处理规则，分别保留文件；五个数据模型单独保留，便于查看字段及维护Flink序列化。历史热更新逻辑放在现有Ignite模块内，新增SQL策略字段；CreatedDate规则、算子UID和keyed state名称保持不变。

## 构建与测试

联网构建机使用 JDK 11 和 Maven 3.9（`mvn -version` 的 Java home 也必须指向 JDK 11）：

```bash
java -version
mvn -version
mvn -B -ntp clean verify
```

产物为 `target/heartbeat-reader-0.1.0.jar`。该 JAR 包含 Kafka Connector 3.3.0-1.20、Kafka客户端、Ignite 2.18.0 thin client 及应用所需 JSON 依赖，不包含 Flink 集群运行时。Jackson 已重定位以避免与集群库冲突。使用此 JAR 时无需额外安装 Kafka SQL Connector，也不依赖 Python。

首次构建需下载 Maven 依赖；离线环境可直接部署构建好的 JAR。若需离线重新构建，应提前准备同一版本的 Maven 依赖缓存与构建插件。

测试中的本地Flink MiniCluster实际运行读取校验、解析、keyBy和侧输出；算子测试通过快照与恢复验证历史序号延续。测试不连接真实Kafka，不能替代Kafka断网、Broker重启、YARN恢复或性能压测。

2026-11-04在Java 11下执行 `mvn clean verify`：152项测试全部通过，失败、错误和跳过均为0。覆盖CreatedDate规则、Flink状态恢复、真实Ignite单表事务/持久化、动态配置后台刷新、历史扩缩容、离线延迟清理、策略版本保护及事务回滚。新增三个端口从解析到SQL存储的41个文档字段验证，以及最新/历史槽位隔离、N+1行数和北京时间跨日的part_day验证。同步程序验证只修改local.properties即可发布、版本自动管理、无效配置保护及凭据隔离；最终JAR经独立Java进程验证配置发布、数据库写入与SQL查询，应用class版本为55。热加载测试使用本地临时文件；HDFS发布和读取、内网Kafka/YARN及吞吐量仍需集群验收。

写入优化版在2026-11-05通过Java 11离线 `mvn -o -B -ntp clean verify`：157项测试全部通过，包含客户端调用数、批量写入回滚、同梯批内去重/乱序/历史扩缩容及并发重叠批次。该结果验证功能和客户端调用减少，不代表已测得集群吞吐上限。

## 配置

复制 `config/reader.example.properties` 为 `config/local.properties`。配置按 UTF-8 读取。可以填写真实值，也可以通过 `${ENV_NAME}` 使用提交进程的环境变量。缺少变量、未知配置键、相同输入/异常Topic、非法阈值会在提交前报错，不打印凭据。

历史保留条数默认10，在 `local.properties` 中设置 `max.records=10`。需要运行中自动生效时，启用 `ignite.history.config.path=hdfs:///flink/projectConfig/faultRecog/heartbeat-history.properties`，在提交节点运行 [配置同步脚本](bin/watch-history-config.sh)。程序监视原始 `local.properties`，自动生成并更新HDFS动态文件、管理版本号；以后只改 `max.records`，不需要填写 `version` 或手动上传文件。同步与容器刷新默认各10秒，无需重启Flink；调小后在各梯下一条新心跳被接受时事务内清理，离线梯暂不清理。完整命令、配置边界及生效查询见 [历史条数热更新](docs/heartbeat-ignite.md#历史条数热更新)。旧键 `ignite.history.max.records` 仍兼容，但不能与 `max.records` 同时配置。

```bash
export HEARTBEAT_KAFKA_BOOTSTRAP='broker1:9092,broker2:9092'
export HEARTBEAT_INPUT_TOPIC='heartbeat-test'
export HEARTBEAT_ERROR_TOPIC='heartbeat-test-errors'
export HEARTBEAT_GROUP_ID='yx2602-heartbeat-preview'
export HEARTBEAT_CHECKPOINT_URI='hdfs:///yx2602/checkpoints/heartbeat-preview'
```

上面的 Broker、Topic、消费组和 HDFS 路径是示例，须按实际环境替换。生产检查点使用各节点可访问的持久化存储；`file:///` 只适合单机测试，不能作为 YARN 容器重建后的可靠恢复存储。

主要可调项：

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| parallelism | 2 | 作业并行度，须结合分区数和压测调整 |
| validation.max.future.skew.ms | 60000 | 最大允许时钟超前 |
| parser.max.age.ms | 600000 | 解析时允许的最大消息年龄，超过则过滤并计数 |
| parser.max.parallelism | 128 | keyed state最大并行度，首次部署后保持不变；不得小于parallelism |
| checkpoint.interval.ms | 30000 | 检查点周期 |
| checkpoint.timeout.ms | 180000 | 检查点超时，必须大于Kafka发送超时 |
| kafka.delivery.timeout.ms | 120000 | 单次生产者发送的总重试时间上限 |
| restart.attempts | 10 | 固定间隔自动恢复次数，耗尽后作业失败并需运维介入 |
| restart.delay.ms | 10000 | 两次恢复之间的等待时间 |
| kafka.error.max.request.bytes | 8388608 | 异常生产者请求上限 |

`kafka.client.*` 只允许连接和认证类参数：`security.*`、`sasl.*`、`ssl.*`、`client.dns.lookup`。禁止通过此入口覆盖自动提交、acks、重试次数、消费组或偏移策略。认证文件路径必须在相关容器中可访问。

## 位点和恢复语义

直接使用 `latest` 存在首次检查点之前的重启窗口：已经读到的异常消息尚未写出时失败，重新取latest可能跳过这些消息。因此启动流程先通过 Kafka AdminClient：

1. 检查输入和异常 Topic 已存在。
2. 查询输入Topic各分区已有的消费组位点。
3. 仅为没有位点的分区查询当前末尾位置，并将该位置保存为初始化位点。
4. KafkaSource 从已提交位点读取；恢复时使用检查点中保存的位点。

初始化提交只表示“从此处开始”，不会确认这之后任何消息已处理。初始化失败则作业不开始读取；已有位点绝不覆盖。同一消费组必须由一个作业独占，不得并发提交两个实例或与外部消费者共用。

关闭运行中的自动分区发现，避免新分区未经初始化就开始消费。输入Topic增加分区后，保存点停止并重新提交，使新增分区先初始化，再开始读取。新增分区初始化前的消息按用户选择的首次latest策略跳过；如需保留，运维应在生产者写入前完成切换，或明确预置起始位点。

KafkaSource 不自动提交；正常消费位点只随成功检查点提交。异常 KafkaSink 使用 `AT_LEAST_ONCE`、`acks=all`、幂等生产者及有时间上限的重试。检查点完成前必须确认待发送异常消息写出，异步写入失败会向任务传播。错误Topic持续不可用时，作业可能阻塞、恢复，最终失败，绝不降级为仅打日志丢弃消息。

Flink内部检查点模式为EXACTLY_ONCE，但异常Topic对外承诺仍为**至少一次**；生产者幂等不能消除任务重放导致的重复。异常消息的 `message_id=topic:partition:offset` 可用于同一源集群/Topic生命周期内去重。Topic删除重建后offset可能复用，归档需同时区分集群和Topic生命周期。

已保存位点超出Kafka保留范围时任务报错，不静默跳到最新。Kafka保留时间必须覆盖预期停机和恢复时间。完整端到端可靠性还依赖后续正常分支的持久化Sink参与同一检查点；仅返回DataStream本身不等于业务已落地。

## 连接读取解析与后续更新模块

```java
ReaderConfig config = ReaderConfig.load(Paths.get("/path/local.properties"));
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
HeartbeatReader.configureEnvironment(env, config);
SingleOutputStreamOperator<HeartbeatMessage> heartbeats = HeartbeatReader.build(env, config);
SingleOutputStreamOperator<ElevatorUpdate> updates = HeartbeatParser.build(heartbeats, config);

IgniteHeartbeatSink.attach(updates, config.ignite); // 同一事务写入最新状态与历史。
// 后续算子使用固定UID。所有分支共用env；最后只执行一次env.execute。
env.execute(config.jobName);
```

该片段对应正式模式。读取和解析的异常Sink均由各自的 `build` 自动挂接，并与Ignite Sink参与同一检查点。`--preview` 入口使用打印Sink展示解析结果。

`HeartbeatMessage` 的字段：

| 字段 | 含义 |
| --- | --- |
| payloadJson | 已验证的完整原始JSON文本，尚未分梯 |
| createdTimeMs | CreatedDate转换后的UTC Unix毫秒，唯一业务时间 |
| sendTimeMs | SendTime为正64位整数时保留，否则null；仅来源元数据 |
| sendTimeRaw | SendTime字段值的JSON表示；字段缺失为null，显式JSON null保存为字符串null |
| receivedAtMs | 校验算子处理时的平台时间，毫秒；并非精确网络接收时刻 |
| sourceTopic / sourcePartition / sourceOffset | Kafka来源位置 |
| kafkaTimestampMs | Kafka记录时间；语义取决于Broker CreateTime/LogAppendTime配置 |

没有添加Watermark或事件时间窗口；后续模块需要窗口时，应按自身业务定义时间戳、水位线和迟到策略。

## YARN Session 预览

预览会消费消息、解析分梯、保存Flink处理状态、写异常Topic并推进消费组位点；正常结果打印单梯JSON，不写入Ignite。必须使用独立测试Topic和消费组。预览使用显式 `--preview`；正式入库使用 `--run`。

在已部署Flink 1.20.5的提交节点，复制JAR和配置文件后：

```bash
source /opt/flink-1.20.5/conf/cdh-env.sh
export JAVA_HOME=/opt/java11
export PATH=/opt/java11/bin:$PATH
export FLINK_APP_ID='application_实际SessionID'

# 先做纯配置检查，不连接Kafka，不提交作业。
java -cp '/home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar:/opt/flink-1.20.5/lib/*' \
  com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  --config /home/xiong/projects/YX2602_FAULTRECOG/config/local.properties --check-config

/opt/flink-1.20.5/bin/flink run \
  -t yarn-session -Dyarn.application.id="$FLINK_APP_ID" -d \
  -c com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  /home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar \
  --config /home/xiong/projects/YX2602_FAULTRECOG/config/local.properties --preview
```

YARN Session 必须由 Java 11 启动；客户端设置 JAVA_HOME 不能升级已经运行的 Session 容器。使用 `flink stop --savepointPath ...` 保存并停止；恢复提交时加 `-s 实际保存点路径`，保持源与状态算子UID不变。不要用删除消费组、重置位点或 `--allowNonRestoredState` 来绕过恢复错误。

## YARN Application 部署

本项目采用Application模式时，直接为心跳作业创建独立YARN应用，无需先启动Session。以下命令为读取与解析预览模式，正式入库请参照 [Ignite部署步骤](docs/heartbeat-ignite.md)：正常结果打印单梯JSON，异常消息实际写Kafka，消费位点实际推进。首次验证使用独立测试消费组；同一消费组不可同时运行Session作业和Application作业。

在提交节点准备以下两个文件：

```text
/home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar
/home/xiong/projects/YX2602_FAULTRECOG/config/local.properties
```

将实际Kafka地址、输入与异常Topic、消费组、检查点路径填写到配置中。首次预览建议 `kafka.group.id=heartbeat-reader-preview`、`checkpoint.storage=hdfs:///flink/checkpoints/heartbeat3-reader-preview`。两个Topic需提前创建；提交账号应有HDFS目录写权限，JobManager与TaskManager节点均需能访问Kafka。

```bash
source /opt/flink-1.20.5/conf/cdh-env.sh
export JAVA_HOME=/opt/java11
export PATH=/opt/java11/bin:$PATH
hdfs dfs -mkdir -p /flink/checkpoints/heartbeat3-reader-preview /flink/savepoints/heartbeat3-reader-preview

java -cp '/home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar:/opt/flink-1.20.5/lib/*' \
  com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  --config /home/xiong/projects/YX2602_FAULTRECOG/config/local.properties --check-config

/opt/flink-1.20.5/bin/flink run-application \
  -t yarn-application \
  -Denv.java.home=/opt/java11 \
  -Dcontainerized.master.env.JAVA_HOME=/opt/java11 \
  -Dcontainerized.taskmanager.env.JAVA_HOME=/opt/java11 \
  -Dclassloader.parent-first-patterns.additional=org.apache.kafka. \
  -Dyarn.application.name=yx2602-heartbeat-reader-preview \
  -Djobmanager.memory.process.size=1024m \
  -Dtaskmanager.memory.process.size=2048m \
  -Dtaskmanager.numberOfTaskSlots=2 \
  -Dyarn.ship-files=/home/xiong/projects/YX2602_FAULTRECOG/config/local.properties \
  -c com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  /home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar \
  --config local.properties --preview
```

所有可能运行容器的节点均须存在 `/opt/java11/bin/java`，且版本为Java 11。两个 `containerized.*.env.JAVA_HOME` 参数分别设置AM/JobManager和TaskManager容器的环境；仅设置提交终端的JAVA_HOME或 `env.java.home` 不能保证容器使用Java 11。

资源配置为小规模联调起点。代码从配置文件的 `parallelism` 读取并行度，示例为2。指定队列时，在JAR路径之前添加 `-Dyarn.application.queue=实际队列名`。首次提交不传 `yarn.application.id`；它由YARN创建后返回。

**配置路径是关键区别：** Application的main在JobManager容器执行，`yarn.ship-files`将提交节点的配置文件上传并本地化为容器工作目录的 `local.properties`，所以应用参数使用 `--config local.properties`，不能使用只在提交节点存在的绝对路径。

若配置含 `${ENV_NAME}`，仅在提交终端export变量不足以让容器继承。应在JobManager容器中配置相应环境，或者使用已填好实际值且访问受控的配置文件。该程序在JobManager解析配置后将连接参数随算子分发；认证证书等本地文件仍须对实际使用它们的容器可见。客户端的 `--check-config` 成功不证明容器环境或网络可用。

记录返回的Application ID，检查状态、作业与日志：

```bash
export FLINK_APP_ID='application_替换为实际ID'
yarn application -status "$FLINK_APP_ID"

/opt/flink-1.20.5/bin/flink list \
  -t yarn-application -Dyarn.application.id="$FLINK_APP_ID"

yarn logs -applicationId "$FLINK_APP_ID"
```

通过YARN Tracking URL进入Flink Web UI。确认作业RUNNING、检查点持续成功，在TaskManager Stdout查找 `heartbeat-parsed-preview` 查看单梯JSON；启动阶段的配置读取、Topic检查和位点初始化异常优先查看JobManager/AM日志。仅收到Application ID不代表作业启动成功。日志尚未聚合时，使用Web UI或容器日志。

保存点停止：

```bash
export FLINK_JOB_ID='替换为实际FlinkJobID'
/opt/flink-1.20.5/bin/flink stop \
  -t yarn-application -Dyarn.application.id="$FLINK_APP_ID" \
  --savepointPath hdfs:///flink/savepoints/heartbeat3-reader-preview \
  "$FLINK_JOB_ID"
```

记录成功返回的savepoint路径。恢复时重新执行上面的完整 `run-application` 命令，在JAR路径之前增加 `-s hdfs:///实际保存点路径`，仍然分发配置文件，并保持消费组、算子UID和兼容代码。新的应用会产生新的Application ID。当前单作业Application在作业停止后会退出；Application模式本身并不启用Flink HA。

以上为依据当前入口及配置编写的部署命令，尚未在内网YARN集群执行验证。

## 异常消息

异常消息是UTF-8 JSON，字段示例：

```json
{
  "schema_version": 1,
  "message_id": "heartbeat-test:2:42",
  "source": {
    "topic": "heartbeat-test",
    "partition": 2,
    "offset": 42,
    "kafka_timestamp_ms": 1793318400000
  },
  "processed_at_ms": 1793318401000,
  "error_code": "MISSING_CREATED_DATE",
  "error_reason": "缺少CreatedDate或值为空",
  "raw_value_base64": "e30=",
  "raw_key_base64": null
}
```

`raw_value_base64` 能无损恢复非法UTF-8；null表示tombstone，空字符串表示空字节。原始key同样保留。错误码包括 `NULL_VALUE`、`INVALID_UTF8`、`INVALID_JSON`、`INVALID_STRUCTURE`、`MISSING_CREATED_DATE`、`INVALID_CREATED_DATE`、`FUTURE_CREATED_DATE`。

上述为读取异常。解析异常使用同一个Topic，增加 `stage=parsing`、`port`、`elevator_id`、`error_field`，保留源位置和原始JSON的Base64；读取层输出不携带Kafka key，解析异常不包含原始key。详见解析说明中的错误码及示例。

预先创建异常Topic并授予Describe/Write权限；消费组还需读取、查询及初始化位点的权限。配置Broker/Topic副本、min.insync.replicas和保留策略，`acks=all`的耐久性依赖实际副本配置。Base64约增加三分之一体积，且异常包同时含key和元数据；Topic的max.message.bytes、Broker限制、复制拉取限制及生产者max.request.size必须容纳最坏情况。超大异常消息写不出时任务会失败，不截断原文继续消费。

## 联调与验收

1. 新测试组启动前放入一条历史记录；初始化后再发送有效心跳，确认前者跳过、后者到正常分支。
2. 发空CreatedDate、非法日期、非法JSON、非法UTF-8和CreatedDate超前60001毫秒的消息，核对异常原文、原因、topic/partition/offset；有效CreatedDate配空SendTime应正常通过。
3. 读取层允许合法历史时间；解析层过滤超过10分钟的消息，不写异常Topic。恰好10分钟及超前60000毫秒的边界以确定时钟的算子测试和单元测试为准。
4. 停止异常Topic写入权限或模拟Broker不可用，确认失败期间成功检查点不前进；恢复后异常消息最终存在，重复允许。
5. 在第一个检查点前终止测试作业，再使用同一消费组提交，验证初始化起点防止已读未交付消息被跳过。
6. 完成检查点后故障恢复，验证正常与异常两分支均可重放；保存点恢复验证UID兼容性。
7. 查询Flink `heartbeat_reader.accepted/rejected` 及解析层 `heartbeat_parser` 指标，结合Kafka消费积压、Sink错误和检查点失败告警。重放会增加计数，它们不是去重后的业务总数。
8. 根据实际分区、消息大小、异常比例和集群资源压测。10万条/秒属于后续包含分梯环节的目标，本模块未宣称达到该指标。
