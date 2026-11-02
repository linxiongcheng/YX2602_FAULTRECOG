# YX2602 心跳数据读取子模块

Java 8 / Flink 1.20.5 实现。读取 Kafka 心跳，校验 JSON 和发送时间，将有效消息交给同一 Flink 作业的后续解析模块，将异常消息可靠写入单独的 Kafka Topic。

## 实现范围

```text
Kafka 原始心跳（bytes，保留来源坐标）
  → KafkaSource<RawHeartbeat>
  → ValidateHeartbeat
      ├─ DataStream<HeartbeatMessage> → 后续解析模块（本期提供接口）
      └─ 异常侧输出 → KafkaSink<String> → 异常 Topic
```

本模块不分梯、不校验 EId 的业务有效性、不比较单梯状态新旧、不写 Ignite3。有效 JSON 中的所有业务字段均原样保存在 `payloadJson`，供解析模块处理。只有 `SendTime` 是本阶段的必填业务字段。

| 约定 | 实现 |
| --- | --- |
| 格式 | UTF-8 JSON 对象，禁止重复键、尾随第二个 JSON、非标准 NaN/Infinity；嵌套深度最多100层 |
| SendTime | JSON 正整数，64位范围，单位为 UTC Unix 毫秒；不接受字符串、浮点数、日期字符串 |
| 未来时间 | 默认最多超前平台时间60000毫秒，恰好60000允许，阈值可配置 |
| 历史时间 | 不限制最早时间，不因消息较旧而丢弃；是否更新状态由后续解析判断 |
| 空消息 | Kafka tombstone、空字节和非法编码都进入异常 Topic |
| 首次启动 | 没有消费位点的分区从初始化时的最新位置开始 |
| 后续启动 | 保留消费组已提交位点；通过 `-s` 恢复时以检查点/保存点状态为准 |
| 异常交付 | 至少一次，允许重放产生重复消息；失败重试，持续失败触发任务恢复 |

**时间单位是上游数据契约。** 不自动猜测秒/毫秒、不用接收时间替代缺失的发送时间。由于允许历史数据，单靠一个正整数不能可靠判断上游是否误传了秒；上线前应核对真实样本。平台时钟用于超前判断，需要节点时钟同步。

## 工程结构

```text
src/main/java/com/hitachi/yx2602/heartbeat/
  ReaderConfig.java             配置加载、环境变量替换、约束检查
  OffsetBootstrap.java          为没有位点的分区持久化首次latest起点
  HeartbeatDeserializer.java    保留Kafka原始字节、key、topic、partition、offset
  HeartbeatValidator.java       可独立测试的格式与时间校验
  ValidateHeartbeat.java        Flink算子、侧输出、指标
  HeartbeatReader.java          KafkaSource、异常KafkaSink、检查点及恢复配置
  HeartbeatReaderJob.java       显式预览入口
  RawHeartbeat.java             源消息POJO
  HeartbeatMessage.java         正常输出POJO
config/reader.example.properties
src/test/java/                  校验、配置、来源信息、作业图、本地Flink运行测试
```

## 构建与测试

联网构建机使用 JDK 8 和 Maven 3.9：

```bash
java -version
mvn -version
mvn -B -ntp verify
```

产物为 `target/heartbeat-reader-0.1.0.jar`。该 JAR 包含 Kafka Connector 3.3.0-1.20、Kafka客户端及应用所需 JSON 依赖，不包含 Flink 集群运行时。Jackson 已重定位以避免与集群库冲突。使用此 JAR 时无需额外安装 Kafka SQL Connector，也不依赖 Python。

首次构建需下载 Maven 依赖；离线环境可直接部署构建好的 JAR。若需离线重新构建，应提前准备同一版本的 Maven 依赖缓存与构建插件。

测试中的本地 Flink MiniCluster 实际运行 Java 校验算子和侧输出，但不连接真实 Kafka。它不能替代 Kafka断网、Broker重启、YARN恢复或性能压测。

2026-10-30本机验证结果：JDK 1.8.0_381 下 `mvn verify` 成功，48项测试全部通过、无跳过；实际运行了本地Flink MiniCluster。打包后的JAR另行通过异常JSON生成和配置入口检查；应用class版本为52（Java 8）。同目录提供JAR的SHA256文件。尚未连接内网Kafka/YARN，启动位点初始化、异常Topic故障恢复及吞吐量需按下文联调步骤验证。

## 配置

复制 `config/reader.example.properties` 为 `config/local.properties`。配置按 UTF-8 读取。可以填写真实值，也可以通过 `${ENV_NAME}` 使用提交进程的环境变量。缺少变量、未知配置键、相同输入/异常Topic、非法阈值会在提交前报错，不打印凭据。

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

## 接入后续解析模块

```java
ReaderConfig config = ReaderConfig.load(Paths.get("/path/local.properties"));
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
HeartbeatReader.configureEnvironment(env, config);
SingleOutputStreamOperator<HeartbeatMessage> heartbeats = HeartbeatReader.build(env, config);

// 在这里接入分梯、字段类型及EId校验、按EId维护状态，以及持久化Sink。
// 后续算子使用固定UID。所有分支共用env；最后只执行一次env.execute。
env.execute(config.jobName);
```

此片段是集成位置示意：实际运行前必须在 `heartbeats` 上接好业务处理与持久化Sink。读取模块的异常Sink已由 `build` 自动挂接，不得另外创建不参与检查点的Python或Java消费者/生产者来替代它。

`HeartbeatMessage` 的字段：

| 字段 | 含义 |
| --- | --- |
| payloadJson | 已验证的完整原始JSON文本，尚未分梯 |
| sendTimeMs | 终端声明的发送时间，毫秒 |
| receivedAtMs | 校验算子处理时的平台时间，毫秒；并非精确网络接收时刻 |
| sourceTopic / sourcePartition / sourceOffset | Kafka来源位置 |
| kafkaTimestampMs | Kafka记录时间；语义取决于Broker CreateTime/LogAppendTime配置 |

没有添加Watermark或事件时间窗口；后续模块需要窗口时，应按自身业务定义时间戳、水位线和迟到策略。

## YARN Session 预览

预览会消费消息、写异常Topic并推进消费组位点；正常消息只打印元数据，不落地业务状态。必须使用独立测试Topic和消费组。入口要求显式 `--preview`，避免把预览误当成生产处理。

在已部署Flink 1.20.5的提交节点，复制JAR和配置文件后：

```bash
source /opt/flink-1.20.5/conf/cdh-env.sh
export FLINK_APP_ID='application_实际SessionID'

# 先做纯配置检查，不连接Kafka，不提交作业。
java -cp '/opt/YX2602_FAULTRECOG/heartbeat-reader-0.1.0.jar:/opt/flink-1.20.5/lib/*' \
  com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  --config /opt/YX2602_FAULTRECOG/config/local.properties --check-config

/opt/flink-1.20.5/bin/flink run \
  -t yarn-session -Dyarn.application.id="$FLINK_APP_ID" -d \
  -c com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  /opt/YX2602_FAULTRECOG/heartbeat-reader-0.1.0.jar \
  --config /opt/YX2602_FAULTRECOG/config/local.properties --preview
```

使用 `flink stop --savepointPath ...` 保存并停止；恢复提交时加 `-s 实际保存点路径`，保持源与状态算子UID不变。不要用删除消费组、重置位点或 `--allowNonRestoredState` 来绕过恢复错误。

## YARN Application 部署

本项目采用Application模式时，直接为心跳作业创建独立YARN应用，无需先启动Session。Java代码和JAR不需要因部署模式切换而修改。当前入口仍为预览模式：正常消息仅打印元数据，异常消息实际写Kafka，消费位点实际推进。首次验证使用独立测试消费组；同一消费组不可同时运行Session作业和Application作业。

在提交节点准备以下两个文件：

```text
/opt/YX2602_FAULTRECOG/heartbeat-reader-0.1.0.jar
/opt/YX2602_FAULTRECOG/config/local.properties
```

将实际Kafka地址、输入与异常Topic、消费组、检查点路径填写到配置中。首次预览建议 `kafka.group.id=heartbeat-reader-preview`、`checkpoint.storage=hdfs:///flink/checkpoints/heartbeat3-reader-preview`。两个Topic需提前创建；提交账号应有HDFS目录写权限，JobManager与TaskManager节点均需能访问Kafka。

```bash
source /opt/flink-1.20.5/conf/cdh-env.sh
hdfs dfs -mkdir -p /flink/checkpoints/heartbeat3-reader-preview /flink/savepoints/heartbeat3-reader-preview

java -cp '/opt/YX2602_FAULTRECOG/heartbeat-reader-0.1.0.jar:/opt/flink-1.20.5/lib/*' \
  com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  --config /opt/YX2602_FAULTRECOG/config/local.properties --check-config

/opt/flink-1.20.5/bin/flink run-application \
  -t yarn-application \
  -Dyarn.application.name=yx2602-heartbeat-reader-preview \
  -Djobmanager.memory.process.size=1024m \
  -Dtaskmanager.memory.process.size=2048m \
  -Dtaskmanager.numberOfTaskSlots=2 \
  -Dyarn.ship-files=/opt/YX2602_FAULTRECOG/config/local.properties \
  -c com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  /opt/YX2602_FAULTRECOG/heartbeat-reader-0.1.0.jar \
  --config local.properties --preview
```

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

通过YARN Tracking URL进入Flink Web UI。确认作业RUNNING、检查点持续成功，正常元数据输出在TaskManager Stdout；启动阶段的配置读取、Topic检查和位点初始化异常优先查看JobManager/AM日志。仅收到Application ID不代表作业启动成功。日志尚未聚合时，使用Web UI或容器日志。

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
  "error_code": "MISSING_SEND_TIME",
  "error_reason": "缺少SendTime或值为空",
  "raw_value_base64": "e30=",
  "raw_key_base64": null
}
```

`raw_value_base64` 能无损恢复非法UTF-8；null表示tombstone，空字符串表示空字节。原始key同样保留。错误码包括 `NULL_VALUE`、`INVALID_UTF8`、`INVALID_JSON`、`INVALID_STRUCTURE`、`MISSING_SEND_TIME`、`INVALID_SEND_TIME`、`FUTURE_SEND_TIME`。

预先创建异常Topic并授予Describe/Write权限；消费组还需读取、查询及初始化位点的权限。配置Broker/Topic副本、min.insync.replicas和保留策略，`acks=all`的耐久性依赖实际副本配置。Base64约增加三分之一体积，且异常包同时含key和元数据；Topic的max.message.bytes、Broker限制、复制拉取限制及生产者max.request.size必须容纳最坏情况。超大异常消息写不出时任务会失败，不截断原文继续消费。

## 联调与验收

1. 新测试组启动前放入一条历史记录；初始化后再发送有效心跳，确认前者跳过、后者到正常分支。
2. 发空SendTime、非法JSON、非法UTF-8和超前60001毫秒的消息，核对异常原文、原因、topic/partition/offset。
3. 发历史时间和恰好超前60000毫秒的消息，确认通过；边界精确性以确定时钟的单元测试为准。
4. 停止异常Topic写入权限或模拟Broker不可用，确认失败期间成功检查点不前进；恢复后异常消息最终存在，重复允许。
5. 在第一个检查点前终止测试作业，再使用同一消费组提交，验证初始化起点防止已读未交付消息被跳过。
6. 完成检查点后故障恢复，验证正常与异常两分支均可重放；保存点恢复验证UID兼容性。
7. 查询Flink `heartbeat_reader.accepted/rejected` 及分原因计数，结合Kafka消费积压、Sink错误和检查点失败告警。重放会增加计数，它们不是去重后的业务总数。
8. 根据实际分区、消息大小、异常比例和集群资源压测。10万条/秒属于后续包含分梯环节的目标，本模块未宣称达到该指标。
