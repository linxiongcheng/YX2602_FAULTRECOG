# heartbeat1、heartbeat2、heartbeat3统一消费

2026-11-13新增多Topic消费。三个Topic使用同一Kafka集群、同一消息结构，由一个Flink作业统一订阅，继续复用现有校验、分梯、新旧判断及Ignite事务Sink。

## 配置

```properties
kafka.input.topic=heartbeat1,heartbeat2,heartbeat3
```

保留现有配置键，值支持逗号分隔的多个Topic，允许每个名称周围有空白。原有 `kafka.input.topic=heartbeat3` 仍兼容。空名称、重复名称、非法名称以及任意输入Topic与异常Topic同名均在配置校验时拒绝。

项目示例和本地local.properties中的输入项已更新为三个Topic，其他连接、消费组、并行度和Ignite参数未因本次功能主动修改。内网需同步修改这一行，保留服务器自己的其他配置。

输入Topic及 `kafka.error.topic` 指向的异常Topic必须事先存在并对运行账号可访问。当前继续共用原配置的异常Topic，例如errors-heartbeat3；读取和解析错误均保留source.topic、partition和offset，无需通过异常Topic名称判断原始来源。

## 数据和状态

- 三个Topic的有效单梯数据写入同一 `PUBLIC.HB_STATE`，表结构、SLOT语义和批量事务方式不变。
- keyBy仍只使用EID。同一个EID即使来自不同Topic，也被视为同一台电梯，共用CreatedDate递增判断、最新状态和历史窗口。若不同设备复用EID，需要先解决标识冲突；本实现不按Topic隔离同号电梯。
- CreatedDate相同或更早的数据仍被过滤，10分钟过期规则保持不变，不使用Topic名称、SendTime或Kafka offset打破时间相同的判断。
- SOURCE_TOPIC保留该次实际接受更新的来源Topic，历史槽位也分别保存对应来源。
- Kafka消费位点以完整 `(topic, partition)` 识别。不同Topic的同号分区不会覆盖位点。
- 启动时一次检查全部输入及异常Topic，随后收集全部输入分区。已有消费组位点保留，仅为没有位点的分区初始化到本次启动时末尾；新增heartbeat1/2默认不回放历史消息。

## 内网部署

本次不删除HB_STATE，不更换Ignite配置，也不重置heartbeat3消费组位点。不生成部署压缩包，由用户在内网构建和提交。

1. 停止旧心跳作业，确认同一消费组没有另一个作业继续运行；本次从单Topic扩大订阅范围时，不携带旧的Savepoint/Checkpoint。
2. 同步源码，在服务器local.properties中修改上述输入Topic项，保留原消费组，以沿用heartbeat3已提交位点。三个Topic应位于同一组bootstrap.servers可访问的Kafka集群。
3. 停止使用当前提交目录JAR的旧日志采集器和统计进程，记下应用及REST信息。用Java 11在内网编译：

   ```bash
   source /opt/flink-1.20.5/conf/cdh-env.sh
   export JAVA_HOME=/opt/java11
   export PATH=/opt/java11/bin:$PATH
   cd /home/xiong/projects/YX2602_FAULTRECOG
   mvn -DskipTests clean package
   ```

4. 直接提交新作业：

   ```bash
   bash /home/xiong/projects/YX2602_FAULTRECOG/bin/start-heartbeat.sh
   ```

   不加 `--from-savepoint`。原因是现有KafkaSource关闭周期分区发现，旧枚举器状态不应被当作发现新订阅Topic的保证。此后已覆盖三个Topic的新版作业，正常恢复或同订阅范围的扩缩容可按既有保存点流程操作；再次新增Topic仍需按本节切换。

沿用原组但不恢复Flink状态，会从Kafka已提交位点开始重读，可能包括已经入库但尚未提交消费位点的消息。Ignite仍用CreatedDate比较防重复更新；本次不保证恢复旧Flink每梯序号。若新输入分区已有消费位点，也会沿用这些位点，而非强制跳到末尾。

## 监控和验收

新日志位点字段包含Topic，避免heartbeat1-0、heartbeat2-0、heartbeat3-0互相混淆：

```text
observed_offset_heartbeat1_p0=...
observed_offset_heartbeat2_p0=...
observed_offset_heartbeat3_p0=...
```

统计报告的Topic/分区列同步识别新字段，也继续读取旧的observed_offset_pN，旧字段标注未注明Topic。三个Topic的同号分区offset没有可比较的大小关系。

校验、解析、写入吞吐默认统计三个Topic合计。新增Topic提高输入总量，当前并行度不自动增大；先核对各Topic分区分配、有效写入量、队列和消息年龄，再根据Ignite处理能力调整。

SQL可核对被接受的更新来源：

```sql
SELECT SOURCE_TOPIC, COUNT(*) FROM PUBLIC.HB_STATE
WHERE SLOT=-1 GROUP BY SOURCE_TOPIC;
```

同梯最新行仅体现最后接受的一条心跳来源，所以某Topic暂时没有最新行不能证明它未消费；应同时核对该Topic消费者位点推进、过滤指标和异常消息。

本地验证包括三Topic配置、全部分区及缺失位点选择、实际KafkaSource订阅列表、真实Flink校验/解析错误来源、三Topic消息经Flink写入同一真实Ignite表、同梯跨Topic最新状态，以及多Topic位点监控和报告。未连接内网Kafka做真实Broker消费验收；该部分由部署后位点和实际消息核对。

Java 11执行 `mvn -o -B -ntp clean verify`，228项测试全部通过，无失败、错误或跳过。未生成或更新dist部署包，未修改内网运行服务。
