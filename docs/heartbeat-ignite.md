# 心跳状态写入 Ignite 2.18.0

本模块将解析层接受的单梯状态写入 Ignite。Java 编译和运行版本统一为11，Flink保持1.20.5，Ignite客户端和服务端均为2.18.0。业务时间统一使用CreatedDate（北京时间），读取、解析算子UID保持不变。时间规则见 [CreatedDate说明](heartbeat-time.md)。

## 写入契约

每台电梯在 `PUBLIC.HB_LATEST` 中有一条最新状态，在 `PUBLIC.HB_HISTORY` 中保留最近N条历史，默认N=10，可在1～1000之间配置并在运行中调整。历史包含当前最新状态；累计不足N条时不填充空行。保留的是成功接受的最近N次状态，不是最近10分钟的全部数据。调小上限后，各电梯在下一条新心跳被接受时清理；离线电梯暂时维持原有历史。

事务使用 Ignite Java thin client 的缓存 API，模式为 `PESSIMISTIC + REPEATABLE_READ`。SQL用于手工建表与查询，不用于跨表事务写入。每批事务按电梯ID排序获取锁，同一电梯的输入顺序保持不变：

1. 事务内读取并锁定最新状态，包括尚不存在的主键。
2. `createdTimeMs <= 数据库最新CREATED_TIME_MS` 时忽略，不递增序号，不写历史。
3. 新记录的 `DB_SEQUENCE = 旧值 + 1`，首条为1。
4. 根据配置版本和该梯已应用版本确定N；N变化时，在事务内取出旧历史、保留最近的至多N−1条，并按新N重排槽位，为新记录预留一格。
5. 历史槽位 `SLOT = (DB_SEQUENCE - 1) % N`，范围0～N−1。同一快照写入最新表和历史槽位，连同策略版本、历史重排一起提交；任何异常都回滚。

数据库序号与 Flink 的 `historySequence` 分开：后者保存为 `FLINK_SEQUENCE`，用于追踪。数据库可能已提交，但对应Flink检查点未完成；重放时依靠数据库CreatedDate时间过滤，不能使用恢复后的Flink序号覆盖历史槽位。

例如N=10时，数据库已经写到第12条，恢复后的Flink重放第9～12条时均跳过；下一个更晚的状态使用数据库序号13，覆盖槽位2。此时数据库保留序号4～13。

## 表与字段

手工执行 [建表脚本](../sql/ignite2-heartbeat.sql)。应用不建表、不自动迁移。当前结构新增 `HISTORY_LIMIT` 和 `HISTORY_POLICY_VERSION`，按新项目建表；旧结构或缺少策略元数据的已有记录不兼容。已有同名表导致脚本报错时，先核对结构，不要直接删表重建。

| 字段 | 含义 |
| --- | --- |
| ELEVATOR_ID | 电梯ID；最新表主键，历史表复合主键的一部分 |
| SLOT | 历史表主键另一部分，0～N−1 |
| CREATED_TIME_MS | CreatedDate按Asia/Shanghai转换的UTC Unix毫秒，唯一新旧比较依据 |
| SEND_TIME_MS | 可选SendTime正整数，否则NULL；不参与时间规则 |
| SEND_TIME_RAW | SendTime字段值的JSON表示，仅追踪用途 |
| DB_SEQUENCE | 数据库成功接受的状态序号 |
| HISTORY_LIMIT | 写入时采用的历史保留上限；最新表记录该梯当前已应用上限 |
| HISTORY_POLICY_VERSION | 写入时采用的策略版本；固定配置为0，动态配置为正整数 |
| FLINK_SEQUENCE | 解析层输出的historySequence |
| PORT | 原心跳端口1～3 |
| RECEIVED_AT_MS / PARSED_AT_MS / STORED_AT_MS | 接收、解析和写入时间，毫秒 |
| SOURCE_TOPIC / SOURCE_PARTITION / SOURCE_OFFSET | Kafka来源坐标 |
| KAFKA_TIMESTAMP_MS | Kafka记录时间 |
| STATE_* | ElevatorState各业务字段，每个字段独立一列 |

业务字段使用 `BIGINT` 或 `VARCHAR`，缺失值保持NULL；不补零、不继承旧值。故障码等字符串保留前导零。字段与类型的完整清单以SQL脚本为准，`STATE_SEC_DOOR_COUTER`、`STATE_TCD_OPEN_DOOR_RUNING` 等名称保留源模型既有拼写。

缓存名分别为 `HB_LATEST`、`HB_HISTORY`，均为 `PARTITIONED + TRANSACTIONAL + FULL_SYNC`，单节点 `BACKUPS=0`，使用 `heartbeat` 数据区域。启动检查缓存存在性、事务模式、同步模式、数据区域名称、无过期策略、表名、字段、主键及二进制类型。程序只访问已有缓存。

客户端无法仅凭缓存配置证明服务端已开启持久化。`--check-ignite` 的成功只证明连接、ACTIVE状态和上述结构符合要求；服务端持久化和WAL设置由现有服务管理员核实。

## 接入已有 Ignite 服务

本项目通过Java thin client连接其他团队维护的Ignite 2.18.0服务。项目部署只需准备业务表、配置连接参数并提交Flink作业；服务端启动、集群激活、内存及磁盘配置由服务管理员负责。

向服务管理员确认以下接入信息：

- 客户端地址、端口（常用10800）、账号密码及TLS要求，确保提交节点和YARN容器能访问。
- 服务处于ACTIVE状态，支持SQL建表及事务缓存，并提供业务表所需的读写权限。
- 业务数据所在区域的持久化、WAL和副本策略，以及是否符合项目对数据恢复的要求。

当前建表脚本指定 `DATA_REGION=heartbeat`、`BACKUPS=0`，代码也校验数据区域名为 `heartbeat`。这些仍是当前实现的要求，需要管理员确认是否适合现有服务；若数据区域或副本策略不同，应先适配脚本和相关校验，不能假定现有服务符合要求。

有建表权限时，使用已安装的SQLLine客户端或管理员提供的SQL工具连接现有服务。以下仅为客户端连接示例，按实际安装路径和服务地址调整：

```bash
/opt/ignite-2.18.0/bin/sqlline.sh \
  -u jdbc:ignite:thin://实际Ignite内网IP:10800
```

在SQLLine提示符执行：

```text
!run /home/xiong/projects/YX2602_FAULTRECOG/sql/ignite2-heartbeat.sql
```

没有建表权限时，将建表脚本交给服务管理员执行。开启认证或TLS时，使用管理员提供的账户、证书与客户端连接参数。后续按下一节配置并运行 `--check-ignite` 验证接入。

## 配置与预检

保留现有 `config/local.properties` 的Kafka地址、消费组和HDFS目录，将示例配置中的Ignite连接参数、历史条数和共享配置路径添加进去，并填写：

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
max.records=10
ignite.history.config.path=hdfs:///flink/config/heartbeat-history.properties
ignite.history.refresh.interval.ms=10000
```

`ignite.retry.attempts` 包含首次尝试。可选 `ignite.username` 和 `ignite.password` 必须同时填写，支持既有 `${ENV_NAME}` 替换。SSL启用时，信任库/密钥库沿用Java SSL系统属性，在实际运行客户端的提交进程、JobManager和TaskManager均需可见；不要把密码放到共享命令历史中。

运行入口：

| 模式 | 行为 |
| --- | --- |
| --check-config | 只检查启动配置，不读取共享动态文件、不连接Kafka/Ignite；允许未配置Ignite地址的预览配置 |
| --check-ignite | 加载动态文件（若启用），检查Ignite连接、ACTIVE状态和表结构；不消费Kafka、不写业务数据 |
| --preview | 读取、解析、打印，正常数据不入库；异常Topic与消费位点仍会更新 |
| --run | 读取、解析并写入Ignite；写入失败触发Flink恢复 |
| --publish-history-config | 在提交节点读取local.properties，按需生成或更新共享动态文件后退出；不连接Kafka/Ignite |
| --watch-history-config | 在提交节点持续监视local.properties中的历史条数，自动发布到共享动态文件；不连接Kafka/Ignite |

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
  -cp "/home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar:/opt/flink-1.20.5/lib/*:${HADOOP_CLASSPATH}:${HADOOP_CONF_DIR}" \
  com.hitachi.yx2602.heartbeat.HeartbeatReaderJob \
  --config /home/xiong/projects/YX2602_FAULTRECOG/config/local.properties --check-ignite
```

启用HDFS动态配置时，先按下一节生成共享文件，再执行预检。直接用Java运行预检需要可用的Hadoop客户端类路径与配置；确保 `cdh-env.sh` 导出的 `HADOOP_CLASSPATH`、`HADOOP_CONF_DIR` 正确，能解析 `hdfs:///` 使用的默认文件系统。Flink的JobManager和TaskManager也须具备HDFS读取权限及相应运行环境。

## 历史条数热更新

只维护提交节点上的 `/home/xiong/projects/YX2602_FAULTRECOG/config/local.properties`，填写 `max.records` 和前述共享路径、刷新间隔。无需填写version，无需自己创建或上传 `heartbeat-history.properties`。旧键 `ignite.history.max.records` 仍兼容，但不能与 `max.records` 同时填写。

YARN的 `yarn.ship-files` 只复制提交时的配置。由提交节点的独立同步进程监视原文件，自动把条数和版本发布到HDFS，再由运行中的写入器读取。共享动态文件不通过 `yarn.ship-files` 分发；同步进程也不能通过 `flink run-application` 放入YARN容器运行。

首次部署新版JAR后，先生成共享文件，成功后再执行数据库预检及原有Flink提交命令：

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/watch-history-config.sh --once
```

然后在提交节点启动持续同步进程（同一HDFS路径只运行一个）：

```bash
nohup bash /home/xiong/projects/YX2602_FAULTRECOG/bin/watch-history-config.sh \
  > /home/xiong/projects/YX2602_FAULTRECOG/heartbeat-history-sync.log 2>&1 < /dev/null &
```

同步进程需要保持运行，退出或提交节点重启后由服务管理工具重新启动；仅执行 `--once` 不会监视后续修改。程序使用源文件旁的 `.history-sync.lock` 加锁，同一台机器重复启动相同配置的同步进程会失败；不要在不同机器或使用不同源文件向同一HDFS路径发布。

以后调为20条，只需编辑上述 `local.properties` 并保存：

```properties
max.records=20
```

程序自动生成更高版本并上传HDFS；条数不变时不重复发布。版本以当前时间和已有共享文件版本为基础严格递增，重启后读取共享文件继续管理。共享文件只有 `version`、`max.records` 和生成注释，不会包含Kafka地址、用户名或密码。不要手工修改、删除或回滚该生成文件。

可以查看同步日志和生成结果：

```bash
tail -n 30 /home/xiong/projects/YX2602_FAULTRECOG/heartbeat-history-sync.log
hdfs dfs -cat /flink/config/heartbeat-history.properties
```

同步进程默认每10秒检查原文件，每个写入器默认再每10秒读取一次共享文件；正常情况下发布与读取合计约0～20秒，再等待该梯的下一条新心跳。刷新间隔由启动时的 `ignite.history.refresh.interval.ms` 设置，最小1000毫秒。成功加载后，下一批事务使用新配置快照；各任务的刷新时刻不同，不保证全局同时切换。某梯接受下一条比库内CreatedDate更新的心跳时，才会应用新的保留上限。重复、乱序旧数据或被解析层过滤的数据不会触发清理。

- 调小：例如20→5，下一条新记录与已有最新4条一起保留，删除与写入在同一事务完成。离线电梯暂不清理。
- 调大：例如5→20，保留现有记录，再逐步积累到20条；已删除的数据不会恢复。
- 同步失败：原文件缺少显式条数、格式非法、条数超出1～1000或HDFS发布失败时，保留已发布的有效配置，记录告警并重试。不要在local.properties中填写version。共享路径改变需重启同步进程及重新提交作业；其他启动参数没有热更新能力。
- 刷新失败：共享文件暂时不可读、格式错误或版本倒退时，写入器继续使用最后有效值并记录告警；首次加载失败则启动失败。
- 版本保护：每台梯在最新表保存已应用版本，刷新较慢或重启的任务不能把它改回旧版本。版本由同步程序自动管理。

不配置 `ignite.history.config.path` 时使用固定的 `max.records`（默认10），修改固定值需要重新提交。启用共享文件后，写入器以同步程序发布的值为准。已经应用动态版本的电梯不会因删除路径配置而退回版本0；后续仍应保持同步程序运行，通过修改源文件条数调整。`file:///` 仅适用于本地测试或所有执行节点都能看到同一文件内容的共享挂载，YARN环境优先使用HDFS。同步脚本要求提交节点可执行 `hdfs dfs`、具备目标目录的创建/读写权限，安全集群还需保持Hadoop身份凭据有效。

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

首次部署直接执行上述命令，不携带旧SendTime版本保存点。后续恢复本项目时，在上述JAR路径之前添加 `-s 实际保存点路径`。CreatedDate状态名为 `last-accepted-created-version-v1`，写入算子UID为 `heartbeat-ignite-sink-v1`。保留原 `parser.max.parallelism`。不要同时运行两个使用同一消费组的作业。

预览消费过的正常消息没有入库。切换正式模式后，从恢复位点继续处理；不会自动补写预览期间的数据，也不会回填超过10分钟的历史。需要已有状态时，应等待设备上报新的心跳。

## 检查点、重试与运维

每个sink子任务有一个写入线程、有界FIFO队列和批次缓冲。默认队列最多1024条、活动批次最多128条；最多等待200ms收集一批。同一电梯在批次内和批次间均保持输入顺序，不合并丢弃中间状态。队列满时阻塞上游，超过flush等待上限则失败恢复。

检查点将屏障排入同一队列，等待此前全部事务提交后才能成功。小批次在等待时间到达、检查点或正常结束时都会提交。取消作业时不主动刷新剩余缓冲；未完成检查点的数据由Flink恢复重放。

连接、超时等客户端异常会关闭旧客户端并有限重试整个事务，重新连接时再次检查结构。若提交成功但响应丢失，重试时CreatedDate时间比较会跳过已提交记录。重试耗尽或后台线程异常都会传播给Flink，不会把数据库不可用当作坏消息发到异常Topic，也不会静默丢弃。

这是**至少一次交付加数据库事务幂等**，不是Kafka与Ignite之间的分布式两阶段提交。数据库可以领先于最后一个完成的检查点；外部查询也不与Flink检查点对齐。单次SQL查询与业务多表读取的隔离要求应由后续查询模块处理。

| 指标 | 含义 |
| --- | --- |
| igniteCommittedRows | 本次运行中确认新接受的行数 |
| igniteSkippedRows | 数据库判定重复或旧状态的行数 |
| igniteQueuedRecords | 队列当前长度，可能含一个检查点屏障，不含活动批次 |
| igniteHistoryMaxRecords | 当前子任务从配置加载的目标历史条数 |
| igniteHistoryConfigVersion | 当前子任务加载的策略版本，固定配置为0 |
| igniteHistoryConfigRefreshFailed | 最近一次刷新失败为1，成功为0 |

指标随作业尝试重置。提交响应丢失后重试可能计为跳过，不能将这些指标当作精确业务账本；以数据库序号为准。

SQLLine中可执行：

```sql
SELECT ELEVATOR_ID, CREATED_TIME_MS, DB_SEQUENCE, STATE_FLOOR, STATE_MAIN_FAULT_CODE
FROM PUBLIC.HB_LATEST ORDER BY ELEVATOR_ID LIMIT 20;

SELECT ELEVATOR_ID, SLOT, CREATED_TIME_MS, DB_SEQUENCE, FLINK_SEQUENCE
FROM PUBLIC.HB_HISTORY WHERE ELEVATOR_ID = 12345 ORDER BY DB_SEQUENCE;

SELECT ELEVATOR_ID, HISTORY_LIMIT, HISTORY_POLICY_VERSION
FROM PUBLIC.HB_LATEST ORDER BY ELEVATOR_ID LIMIT 20;

SELECT H.ELEVATOR_ID, COUNT(*) AS HISTORY_COUNT, L.HISTORY_LIMIT
FROM PUBLIC.HB_HISTORY H JOIN PUBLIC.HB_LATEST L ON H.ELEVATOR_ID=L.ELEVATOR_ID
GROUP BY H.ELEVATOR_ID, L.HISTORY_LIMIT HAVING COUNT(*) > L.HISTORY_LIMIT;
```

将12345替换为真实电梯ID，最后一个查询在写入稳定时应返回空结果。环形槽位不是时间排序，查询历史必须按DB_SEQUENCE或CREATED_TIME_MS排序。最新表的策略字段代表该梯实际已应用配置；子任务指标仅代表已加载目标，离线电梯可能仍使用旧版本。重排时保留的历史行携带原写入时的策略字段，不能用这些旧历史字段判断当前上限。

## 验证范围

时间规则及验证说明见 [CreatedDate说明](heartbeat-time.md)。

本地测试包含真实Ignite 2.18.0持久化节点、SQL建表、Java thin client和Flink MiniCluster。覆盖空值/前导零、循环历史、数据库与Flink序号分离、重放过滤、同梯并发首次写入、批次中途失败回滚、持久化重启、检查点提交等待、数据库中断后的检查点失败与恢复、队列背压及正常结束刷新。动态配置通过临时本地文件验证后台热加载、10→20→5→1→4扩缩容、离线延迟清理、无效配置保留旧值、旧任务版本保护，以及历史重排与策略元数据的事务回滚。同步程序验证只修改local.properties即可发布并被写入器读取、版本自动递增及重启延续、拒绝重复同步进程、缺少条数或无效配置不覆盖有效文件、生成文件不包含凭据，以及最终JAR独立运行配置发布入口。

HDFS动态文件读取及权限、内网Kafka、YARN容器网络、集群类加载配置、服务端进程崩溃/断电，以及10万条/秒吞吐量仍需集群验收。批次事务涉及多次网络调用，调整上限时还需重排该梯历史；当前默认资源与批大小是联调配置，不能据此承诺生产吞吐量。
