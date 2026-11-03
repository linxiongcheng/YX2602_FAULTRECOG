# 心跳状态写入 Ignite 2.18.0

本模块将解析层接受的单梯状态写入 Ignite。Java 编译和运行版本统一为11，Flink保持1.20.5，Ignite客户端和服务端均为2.18.0。读取、解析规则及已有状态UID保持不变。

## 写入契约

每台电梯在 `PUBLIC.HB_LATEST` 中有一条最新状态，在 `PUBLIC.HB_HISTORY` 中最多有10条历史。历史包含当前最新状态；累计不足10条时不填充空行。保留的是成功接受的最近10次状态，不是最近10分钟的全部数据。

事务使用 Ignite Java thin client 的缓存 API，模式为 `PESSIMISTIC + REPEATABLE_READ`。SQL用于手工建表与查询，不用于跨表事务写入。每批事务按电梯ID排序获取锁，同一电梯的输入顺序保持不变：

1. 事务内读取并锁定最新状态，包括尚不存在的主键。
2. `SendTime <= 数据库最新SendTime` 时忽略，不递增序号，不写历史。
3. 新记录的 `DB_SEQUENCE = 旧值 + 1`，首条为1。
4. 历史槽位 `SLOT = (DB_SEQUENCE - 1) % 10`，范围0～9。
5. 同一快照写入最新表和历史槽位，整个批次一并提交；任何异常都回滚。

数据库序号与 Flink 的 `historySequence` 分开：后者保存为 `FLINK_SEQUENCE`，用于追踪。数据库可能已提交，但对应Flink检查点未完成；重放时依靠数据库发送时间过滤，不能使用恢复后的Flink序号覆盖历史槽位。

例如数据库已经写到第12条，恢复后的Flink重放第9～12条时均跳过；下一个更晚的状态使用数据库序号13，覆盖槽位2。数据库中始终保留序号4～13。

## 表与字段

手工执行 [建表脚本](../sql/ignite2-heartbeat.sql)。应用不建表、不自动迁移。已有同名表导致脚本报错时，先核对结构，不要直接删表重建。

| 字段 | 含义 |
| --- | --- |
| ELEVATOR_ID | 电梯ID；最新表主键，历史表复合主键的一部分 |
| SLOT | 历史表主键另一部分，0～9 |
| SEND_TIME_MS | 上游发送时间，毫秒 |
| DB_SEQUENCE | 数据库成功接受的状态序号 |
| FLINK_SEQUENCE | 解析层输出的historySequence |
| PORT | 原心跳端口1～3 |
| RECEIVED_AT_MS / PARSED_AT_MS / STORED_AT_MS | 接收、解析和写入时间，毫秒 |
| SOURCE_TOPIC / SOURCE_PARTITION / SOURCE_OFFSET | Kafka来源坐标 |
| KAFKA_TIMESTAMP_MS | Kafka记录时间 |
| STATE_* | ElevatorState各业务字段，每个字段独立一列 |

业务字段使用 `BIGINT` 或 `VARCHAR`，缺失值保持NULL；不补零、不继承旧值。故障码等字符串保留前导零。字段与类型的完整清单以SQL脚本为准，`STATE_SEC_DOOR_COUTER`、`STATE_TCD_OPEN_DOOR_RUNING` 等名称保留源模型既有拼写。

缓存名分别为 `HB_LATEST`、`HB_HISTORY`，均为 `PARTITIONED + TRANSACTIONAL + FULL_SYNC`，单节点 `BACKUPS=0`，使用 `heartbeat` 数据区域。启动检查缓存存在性、事务模式、同步模式、数据区域名称、无过期策略、表名、字段、主键及二进制类型。程序只访问已有缓存。

客户端无法仅凭缓存配置证明服务端已开启持久化。`--check-ignite` 的成功只证明连接、ACTIVE状态和上述结构符合要求；服务端持久化和WAL设置必须按以下步骤核实。

## 单节点持久化部署

以下示例假定 Ignite 2.18.0 发行包位于 `/opt/ignite-2.18.0`，项目位于 `/home/xiong/projects/YX2602_FAULTRECOG`。如果发行包目录不同，只调整Ignite程序路径。不要继续使用Ignite3的启动程序或配置格式。

已确认现有节点没有需要保留的数据。本次使用全新的 `/home/xiong/ignite-data/heartbeat` 目录，不提供删除旧目录的命令。停止原Ignite进程后，在服务端编辑 `config/ignite2-single-node.xml`：将两处 `127.0.0.1` 改为本节点可被集群访问的内网IP；固定 `consistentId` 和存储路径，后续重启不要改变。

模板启用 Native Persistence 和 `WALMode=FSYNC`，持久化目录包含数据、WAL和WAL归档；内存区域初始256MiB、最大1GiB，仅作为联调起点。目录应位于可靠的本地磁盘，不能放在YARN临时容器目录。不要禁用两张表的WAL。

```bash
mkdir -p /home/xiong/ignite-data/heartbeat
export JAVA_HOME=/opt/java11
export PATH=/opt/java11/bin:$PATH

/opt/ignite-2.18.0/bin/ignite.sh \
  /home/xiong/projects/YX2602_FAULTRECOG/config/ignite2-single-node.xml
```

该命令在前台运行。服务管理方式沿用集群运维规范，确保重启仍加载同一个配置。服务端须提供 `ignite-spring` 和 `ignite-indexing` 模块（发行包的libs中）；如果indexing在 `libs/optional` 中，按发行包说明启用，SQL建表与查询依赖它。

首次启用持久化后，在另一个终端激活集群。下面命令在Ignite服务端执行，将 `实际Ignite内网IP` 替换为配置中的地址：

```bash
export JAVA_HOME=/opt/java11
export PATH=/opt/java11/bin:$PATH
/opt/ignite-2.18.0/bin/control.sh \
  --host 实际Ignite内网IP --port 10800 --set-state ACTIVE --yes
```

启动SQLLine，连接该节点：

```bash
/opt/ignite-2.18.0/bin/sqlline.sh \
  -u jdbc:ignite:thin://实际Ignite内网IP:10800
```

在SQLLine提示符执行：

```text
!run /home/xiong/projects/YX2602_FAULTRECOG/sql/ignite2-heartbeat.sql
```

开启认证或TLS时，使用部署环境的账户、证书与连接参数。默认模板未开启认证/TLS，连接端口应限制在集群内网。

持久化保障进程重启后的恢复；单节点没有故障切换能力，机器或磁盘丢失不能靠本配置恢复。

## 配置与预检

保留现有 `config/local.properties` 的Kafka地址、消费组和HDFS目录，将示例配置中的 `ignite.*` 部分添加进去，并填写：

```properties
ignite.addresses=实际Ignite内网IP:10800
ignite.ssl.enabled=false
ignite.request.timeout.ms=10000
ignite.transaction.timeout.ms=10000
ignite.retry.attempts=3
ignite.retry.delay.ms=1000
ignite.batch.size=128
ignite.queue.capacity=1024
ignite.batch.linger.ms=200
ignite.flush.timeout.ms=60000
```

`ignite.retry.attempts` 包含首次尝试。可选 `ignite.username` 和 `ignite.password` 必须同时填写，支持既有 `${ENV_NAME}` 替换。SSL启用时，信任库/密钥库沿用Java SSL系统属性，在实际运行客户端的提交进程、JobManager和TaskManager均需可见；不要把密码放到共享命令历史中。

运行入口：

| 模式 | 行为 |
| --- | --- |
| --check-config | 只检查配置，不连接Kafka/Ignite；允许未配置Ignite地址的预览配置 |
| --check-ignite | 检查Ignite连接、ACTIVE状态和表结构；不消费Kafka、不写业务数据 |
| --preview | 读取、解析、打印，正常数据不入库；异常Topic与消费位点仍会更新 |
| --run | 读取、解析并写入Ignite；写入失败触发Flink恢复 |

在提交节点执行数据库预检（不能代替YARN容器内的网络检查）：

```bash
source /opt/flink-1.20.5/conf/cdh-env.sh
export JAVA_HOME=/opt/java11
export PATH=/opt/java11/bin:$PATH

/opt/java11/bin/java \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  -cp '/home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar:/opt/flink-1.20.5/lib/*' \
  com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  --config /home/xiong/projects/YX2602_FAULTRECOG/config/local.properties --check-ignite
```

## YARN Application正式运行

所有可能运行JobManager/TaskManager的节点均须安装Java 11到 `/opt/java11`。提交终端的JAVA_HOME不能替代容器内的Java路径。保持集群现有Kafka父加载优先参数，其客户端兼容性仍需集群验证。

```bash
source /opt/flink-1.20.5/conf/cdh-env.sh
export JAVA_HOME=/opt/java11
export PATH=/opt/java11/bin:$PATH

hdfs dfs -mkdir -p /flink/checkpoints/heartbeat3-reader /flink/savepoints/heartbeat3-reader

/opt/flink-1.20.5/bin/flink run-application \
  -t yarn-application \
  -Denv.java.home=/opt/java11 \
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
```

如果集群已有 `env.java.opts.all` 必需参数，应合并，不能直接覆盖丢弃。Application模式下配置通过 `yarn.ship-files` 本地化，因此应用参数中的配置路径保持 `local.properties`。

升级旧作业时先保存点停止，再替换JAR；在上述JAR路径之前添加 `-s 实际保存点路径` 恢复。读取和解析UID、keyed state名称及序列化模型保持不变，新增写入算子UID为 `heartbeat-ignite-sink-v1`。保留原 `parser.max.parallelism`。不要同时运行两个使用同一消费组的作业。

预览消费过的正常消息没有入库。切换正式模式后，从恢复位点继续处理；不会自动补写预览期间的数据，也不会回填超过10分钟的历史。需要已有状态时，应等待设备上报新的心跳。

## 检查点、重试与运维

每个sink子任务有一个写入线程、有界FIFO队列和批次缓冲。默认队列最多1024条、活动批次最多128条；最多等待200ms收集一批。同一电梯在批次内和批次间均保持输入顺序，不合并丢弃中间状态。队列满时阻塞上游，超过flush等待上限则失败恢复。

检查点将屏障排入同一队列，等待此前全部事务提交后才能成功。小批次在等待时间到达、检查点或正常结束时都会提交。取消作业时不主动刷新剩余缓冲；未完成检查点的数据由Flink恢复重放。

连接、超时等客户端异常会关闭旧客户端并有限重试整个事务，重新连接时再次检查结构。若提交成功但响应丢失，重试时发送时间比较会跳过已提交记录。重试耗尽或后台线程异常都会传播给Flink，不会把数据库不可用当作坏消息发到异常Topic，也不会静默丢弃。

这是**至少一次交付加数据库事务幂等**，不是Kafka与Ignite之间的分布式两阶段提交。数据库可以领先于最后一个完成的检查点；外部查询也不与Flink检查点对齐。单次SQL查询与业务多表读取的隔离要求应由后续查询模块处理。

| 指标 | 含义 |
| --- | --- |
| igniteCommittedRows | 本次运行中确认新接受的行数 |
| igniteSkippedRows | 数据库判定重复或旧状态的行数 |
| igniteQueuedRecords | 队列当前长度，可能含一个检查点屏障，不含活动批次 |

指标随作业尝试重置。提交响应丢失后重试可能计为跳过，不能将这些指标当作精确业务账本；以数据库序号为准。

SQLLine中可执行：

```sql
SELECT ELEVATOR_ID, SEND_TIME_MS, DB_SEQUENCE, STATE_FLOOR, STATE_MAIN_FAULT_CODE
FROM PUBLIC.HB_LATEST ORDER BY ELEVATOR_ID LIMIT 20;

SELECT ELEVATOR_ID, SLOT, SEND_TIME_MS, DB_SEQUENCE, FLINK_SEQUENCE
FROM PUBLIC.HB_HISTORY WHERE ELEVATOR_ID = 12345 ORDER BY DB_SEQUENCE;

SELECT ELEVATOR_ID, COUNT(*) FROM PUBLIC.HB_HISTORY
GROUP BY ELEVATOR_ID HAVING COUNT(*) > 10;
```

将12345替换为真实电梯ID，最后一个查询应返回空结果。环形槽位不是时间排序，查询历史必须按DB_SEQUENCE或发送时间排序。

## 验证范围

2026-11-03在Java 11下执行 `mvn clean verify`：108项测试通过，失败/错误/跳过均为0。最终JAR已由独立Java进程实际连接Ignite完成写入和查询，服务端XML模板通过Spring实际加载。

本地测试包含真实Ignite 2.18.0持久化节点、SQL建表、Java thin client和Flink MiniCluster。覆盖空值/前导零、循环历史、数据库与Flink序号分离、重放过滤、同梯并发首次写入、批次中途失败回滚、持久化重启、检查点提交等待、数据库中断后的检查点失败与恢复、队列背压及正常结束刷新。

内网Kafka、YARN容器网络、集群类加载配置、服务端进程崩溃/断电，以及10万条/秒吞吐量仍需集群验收。批次事务涉及多次网络调用，当前默认资源与批大小是联调配置，不能据此承诺生产吞吐量。
