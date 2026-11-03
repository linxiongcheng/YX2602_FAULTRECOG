# 心跳解析模块实现与部署

本模块将读取层的 `HeartbeatMessage` 拆成单梯 `ElevatorUpdate`。使用Java 11、Flink 1.20.5，沿用已有YARN Application入口和异常Topic。当前预览会执行读取、解析和状态更新，输出完整单梯JSON；正式模式 `--run` 已接入 [Ignite 2.18.0 写入模块](heartbeat-ignite.md)，故障真实性判别尚未实现。

业务日期使用 `yyyy/M/d H:mm:ss` 格式，可选1～3位小数秒，时区固定Asia/Shanghai。缺失、空白、日期非法或超过未来偏差进入读取异常Topic；不回退到其他时间。同一电梯CreatedDate相同时只接受首条，即使两条SendTime不同。

## 处理顺序

1. 使用平台处理时间检查由 `CreatedDate` 转换的 `createdTimeMs`，超过 `parser.max.age.ms`（默认600000毫秒）就过滤整条心跳并计数，不写异常Topic。
2. 按端口1、2、3识别电梯数据。`EId=0`（含字符串`"0"`）为空端口，跳过该组所有字段。完全没有该端口的已知字段时视为未提供端口；已提供业务字段但缺少EId，或EId显式为null时，记录该端口异常。
3. 每个非空端口独立检查字段。第二台异常不会影响第一台和第三台。公共字段非法时，会使引用它的每个非空端口分别产生异常。
4. 缺失/为null的可选字段保持null。合法十进制整数字符串允许转换，可带正负号和两侧空格；小数、指数形式字符串、布尔值、空字符串、64位溢出或超出字段范围的值拒绝。不自动填0，不沿用上一条记录的非空字段。
5. 按 `elevatorId` 执行 `keyBy`。在分组后再次检查10分钟年龄，避免shuffle/反压使消息超时后仍被接受。
6. 比较Flink状态中的最后接受时间。同时间算重复，更早算旧消息；两者均过滤、计数，不写异常Topic，也不增加历史序号。字段异常先于该步骤处理，因此不能靠重复时间掩盖字段错误。
7. 时间更新且有效时，更新最后接受时间，历史序号从1递增，输出完整单梯快照和Kafka来源信息。每台电梯独立编号。

10分钟是消息年龄，不是设备离线时长。平台停机、队列积压造成的旧消息同样会被过滤。恰好600000毫秒允许，600001毫秒过滤。终端允许的60秒时钟超前仍由读取层检查。

如果一条心跳在两个非空端口使用相同EId，将这两个端口分别标记 `DUPLICATE_ELEVATOR_ID`，避免任意挑选其中一个状态；不影响其他编号的端口。所有端口都明确为0是正常空消息；完全没有可识别的电梯端口数据则为 `NO_ELEVATOR_DATA`。

## 字段映射与范围

其中n为1、2或3。以下业务字段均允许缺失或为null，EId和CreatedDate除外。输出数字字段使用可空Long，保持状态标志0/1的语义，不转换为真假值。

| 输入 | 输出 | 检查规则 |
| --- | --- | --- |
| EIdn | elevatorId | 正64位整数；0表示空端口 |
| CreatedDate | createdTimeMs、state.createdDate | 必填北京时间字符串，严格解析为Unix毫秒并核对读取结果；保留原始日期文本 |
| SendTime | sendTimeMs、sendTimeRaw | 可选来源元数据；正64位整数保留为sendTimeMs，其他情况为null；sendTimeRaw保留字段值的JSON表示 |
| IntId、FId | state.intId、state.fId | 非负64位整数 |
| Code | state.code | 非空字符串原样保留；非负64位整数转字符串 |
| Floorn | state.floor | 有符号32位整数，支持负楼层 |
| ACDn、Directionn | state.acd、state.direction | 非负32位整数；不推测未明确的ACD和方向枚举 |
| RSn、DoorLockn | state.rs、state.doorLock | 0或1 |
| IsActiven、IsDebugn、IsRepairn | state.isActive、state.isDebug、state.isRepair | 0或1 |
| IsPowern、IsDoorn、IsHardwaren、IsCalln | 对应state小驼峰字段 | 0或1 |
| RunTotaln、RunTimen | state.runTotal、state.runTime | 非负64位整数，支持超过有符号32位范围的累计数 |
| Speedn、Weightn | state.speed、state.weight | 非负64位整数，保留原始单位，不推测缩放系数 |
| MainFaultCoden、SubFaultCoden | state.mainFaultCode、state.subFaultCode | 字符串原样保留，包括前导零；非负64位整数转字符串 |
| DoorCountern、SecDoorCoutern | state.doorCounter、state.secDoorCouter | 非负64位整数；SecDoorCouter沿用源协议拼写 |
| TCDAn、TCDBn、TCDCn | state.tcda、state.tcdb、state.tcdc | 0或1 |
| TCDStopOutsideDoorn、TCDOpenDoorRuningn | state.tcdStopOutsideDoor、state.tcdOpenDoorRuning | 0或1；Runing沿用源协议拼写 |
| TCDEntrapmentn、TCDDoorn | state.tcdEntrapment、state.tcdDoor | 0或1 |
| SendCount、ReceiveCount | state.sendCount、state.receiveCount | 非负64位整数 |
| SignalRssi | state.signalRssi | 0～31 |
| NetworkType | state.networkType | 1或2，按现有2G/4G协议定义 |
| RecordStatus、Status | state.recordStatus、state.status | 有符号32位整数；不补文档中的默认值 |
| UpdatedDate | state.updatedDate | 原始可空字符串，不参与时间排序 |

未知字段不会导致异常，也不自动扩展单梯输出结构；新增业务字段需显式增加映射与测试。数仓字段 `part_day` 不属于本实时消息合同，本模块不生成数仓分区。

## 输出与状态

`ElevatorUpdate` 包含：`elevatorId`、`port`、`createdTimeMs`、`sendTimeMs`、`sendTimeRaw`、`historySequence`、`receivedAtMs`、`parsedAtMs`、`sourceTopic`、`sourcePartition`、`sourceOffset`、`kafkaTimestampMs`、`state`。

预览在TaskManager Stdout中以 `heartbeat-parsed-preview` 为前缀。下面是输出节选；实际包含所有定义的state字段，缺失字段为null：

```json
{
  "elevatorId": 773282,
  "port": 1,
  "createdTimeMs": 1793318400000,
  "sendTimeMs": null,
  "sendTimeRaw": "\"\"",
  "historySequence": 1,
  "receivedAtMs": 1793318401000,
  "parsedAtMs": 1793318401005,
  "sourceTopic": "heartbeat3",
  "sourcePartition": 2,
  "sourceOffset": 42,
  "kafkaTimestampMs": 1793318400000,
  "state": {"floor": 11, "speed": 98, "mainFaultCode": "0007", "weight": null}
}
```

每台电梯仅持久化最后接受时间和历史序号两个long，不保存所有历史心跳，也不启用状态TTL。离线45天后恢复，旧补发数据过滤，当前新数据继续编号。序号溢出会使任务失败，不能回绕。

状态通过Flink检查点/保存点保存。恢复时必须使用兼容检查点或 `-s 保存点路径`；只沿用Kafka消费组位点但不恢复Flink状态，会失去历史序号和比较基准。首次启动每梯序号从1开始。项目不兼容旧SendTime版本保存点。

固定算子UID为 `heartbeat-split-v1`、`heartbeat-elevator-ordering-v1`、`heartbeat-parser-error-kafka-v1`；状态名为 `last-accepted-created-version-v1`，使用CreatedVersion保存业务时间和序号。`parser.max.parallelism` 默认128，已有保存点后不要随意修改。正常并行度可以在该上限内调整，实际集群扩缩容仍需验证。

存储模块使用一条输出同时构造最新状态和历史写入，不会为两种写入分别递增序号。数据库独立维护DB_SEQUENCE，按 `(DB_SEQUENCE - 1) % N` 定位历史槽位；N默认为10，支持通过共享配置热更新，详见 [存储模块说明](heartbeat-ignite.md#历史条数热更新)。使用EId和CreatedDate对应的时间进行幂等判断。Flink的historySequence仅作追踪，不能作为数据库历史槽位依据。

## 解析异常

使用现有异常Topic，独立KafkaSink沿用至少一次交付、发送失败传播和检查点约束。输出字段：

- `stage=parsing`，区分读取阶段异常。
- `port`：1～3为具体端口，0为整条消息结构错误。
- `elevator_id`：可解析的电梯编号；无法确定时为null。
- `error_code`、`error_field`、`error_reason`：错误位置和原因，每个异常端口输出首个错误。
- `source`、`processed_at_ms`、`raw_value_base64`：来源位置、处理时间和完整输入JSON。
- `message_id`：源Topic、分区、offset、端口、错误码的组合，允许重放后重复投递。

主要错误码：`INVALID_ELEVATOR_ID`、`INVALID_FIELD`、`DUPLICATE_ELEVATOR_ID`、`NO_ELEVATOR_DATA`。结构防御检查还可能输出 `INVALID_JSON`、`INVALID_STRUCTURE`、`INVALID_CREATED_DATE`。读取层已经校验过的正常链路通常不会触发这些结构错误。

## 配置与升级运行

时间规则不增加必填配置项，现有 `local.properties` 可继续使用；按新项目首次部署，时间规则见 [CreatedDate说明](heartbeat-time.md)。需要显式配置时加入：

```properties
parser.max.age.ms=600000
parser.max.parallelism=128
```

在开发机执行 `mvn clean verify`，将新的 `target/heartbeat-reader-0.1.0.jar` 上传到集群同名位置。首次部署不加载旧项目保存点；本项目后续更新时先保存点停止再替换JAR，不要让两个作业共用消费组同时运行。版本号沿用原项目配置，部署时可核对随包SHA256。

入口类、`--config local.properties`、`--preview` 参数和YARN Application部署方式保持兼容。最新预览入口会执行解析，不再只是显示读取元数据。当前 `parallelism` 必须不超过新增的 `parser.max.parallelism`。

用户集群已使用的Kafka父加载优先参数可继续放在提交命令中：

```bash
source /opt/flink-1.20.5/conf/cdh-env.sh
export JAVA_HOME=/opt/java11
export PATH=/opt/java11/bin:$PATH
/opt/flink-1.20.5/bin/flink run-application \
  -t yarn-application \
  -Denv.java.home=/opt/java11 \
  -Dyarn.application.name=yx2602-heartbeat-parser-preview \
  -Djobmanager.memory.process.size=1024m \
  -Dtaskmanager.memory.process.size=2048m \
  -Dtaskmanager.numberOfTaskSlots=2 \
  -Dclassloader.parent-first-patterns.additional=org.apache.kafka. \
  -Dyarn.ship-files=/home/xiong/projects/YX2602_FAULTRECOG/config/local.properties \
  -c com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  /home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar \
  --config local.properties --preview
```

需要恢复时，在JAR路径前添加 `-s 实际保存点路径`。父加载优先参数依赖集群实际Kafka客户端版本兼容性，本地测试不证明集群类路径配置正确。

## 指标与验证

`heartbeat_parser` 指标分为两组：

| 指标 | 单位与含义 |
| --- | --- |
| expired_heartbeats | 整条心跳在分梯前已超过年龄阈值 |
| empty_ports / absent_ports | EId=0的端口 / 完全未提供的端口 |
| rejected_records | 解析异常记录数，包括端口异常和整条结构异常 |
| parsed_elevators | 通过字段校验、尚未进行单梯新旧过滤的记录数 |
| expired_elevators | keyBy后因积压或反压超过年龄阈值的记录数 |
| duplicate_elevators / older_elevators | 与上次时间相同 / 更早的记录数 |
| accepted_elevators | 已生成历史序号并输出的单梯记录数 |

测试覆盖第一/第三台正常而第二台异常、数字字符串、前导零故障码、可空字段、非法范围、异常原文恢复、重复ID、10分钟边界、过期过滤不进异常Topic、重复/乱序不增序号、独立电梯编号、检查点恢复与长期离线后继续编号，以及真实本地Flink运行。

2026-11-03在Java 11下执行 `mvn verify`：143项测试全部通过，失败、错误和跳过均为0。覆盖CreatedDate北京时间解析、空SendTime、两种时间顺序冲突、过期和未来边界、Flink状态恢复，以及真实Ignite事务/持久化测试。新增动态配置后台刷新、历史扩缩容、离线延迟清理、配置异常保留旧值、策略版本保护及调整历史时的事务回滚验证。最终JAR经独立Java进程验证写入与SQL查询，应用class版本为55。热加载测试使用本地临时文件；HDFS、内网Kafka/YARN及吞吐量仍需集群验收。

尚未在真实Kafka/YARN集群运行本版，也未验证10万条/秒。集群验收应检查解析输出、错误Topic、上述指标、保存点恢复，以及持久化链路的端到端一致性。
