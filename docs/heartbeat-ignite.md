# 心跳状态写入 Ignite 2.18.0

本模块将解析层接受的单梯状态写入 Ignite。Java 编译和运行版本统一为11，Flink保持1.20.5，Ignite客户端和服务端均为2.18.0。业务时间统一使用CreatedDate（北京时间），读取、解析算子UID保持不变。时间规则见 [CreatedDate说明](heartbeat-time.md)。

旧表升级到无需引号的字段结构，请按 [清空重建与重新部署](redeploy-unquoted-columns.md) 操作。必须同时更新建表脚本和JAR，不能只改SQL文件。

## 正式部署顺序

以下步骤在集群提交节点执行，项目固定放在 `/home/xiong/projects/YX2602_FAULTRECOG`。本项目连接已有Ignite服务，服务端由管理员维护。

1. [准备部署文件](#准备部署文件)，确认所有可能运行YARN容器的节点均有Java 11。
2. [接入已有Ignite服务](#接入已有-ignite-服务)，由有权限的人员核对并创建业务表。
3. [填写配置](#配置与预检)，检查Kafka Topic、消费组、Ignite连接参数和HDFS目录。
4. [初始化并启动历史配置同步](#历史条数热更新)，成功生成共享文件后执行配置与数据库预检。
5. [提交YARN Application](#yarn-application正式运行)，正式模式使用 `--run`。
6. [检查作业和入库结果](#运行验收与日志)，确认RUNNING、检查点成功、业务数据持续更新。
7. 后续维护按 [Checkpoint与Savepoint](#checkpoint与savepoint) 保存并恢复状态，启动失败按 [常见部署问题](#常见部署问题) 提取日志。

## 写入契约

所有状态存入一张 `PUBLIC.HB_STATE`，复合主键为 `(EID, SLOT)`。每台电梯的 `SLOT=-1` 固定保存最新状态，非负槽位保存最近N条历史；N默认10，可在1～1000之间配置并在运行中调整。历史包含当前最新状态，因此每梯最多N+1行；累计不足N条时不填充空行。保留的是成功接受的最近N次状态，不是最近10分钟的全部数据。调小上限后，各电梯在下一条新心跳被接受时清理；离线电梯暂时维持原有历史。

事务使用 Ignite Java thin client 的缓存 API，模式为 `PESSIMISTIC + REPEATABLE_READ`。SQL用于手工建表与查询。每批事务按电梯ID排序获取锁，同一电梯的输入顺序保持不变：

1. 事务内读取并锁定该梯 `SLOT=-1` 的最新状态，包括尚不存在的复合主键；两个并发首次写入也按这个固定键串行处理。
2. `createdTimeMs <= 数据库最新CREATED_TIME_MS` 时忽略，不递增序号，不写历史。
3. 新记录的 `DB_SEQUENCE = 旧值 + 1`，首条为1。
4. 根据配置版本和该梯已应用版本确定N；N变化时，在事务内取出旧历史、保留最近的至多N−1条，并按新N重排槽位，为新记录预留一格。
5. 历史槽位 `SLOT = (DB_SEQUENCE - 1) % N`，范围0～N−1。同一快照写入 `SLOT=-1` 和对应历史槽位，连同策略版本、历史重排一起提交；任何异常都回滚。调整历史时只操作非负槽位，不能删除最新状态的固定槽位。

数据库序号与 Flink 的 `historySequence` 分开：后者保存为 `FLINK_SEQUENCE`，用于追踪。数据库可能已提交，但对应Flink检查点未完成；重放时依靠数据库CreatedDate时间过滤，不能使用恢复后的Flink序号覆盖历史槽位。

例如N=10时，数据库已经写到第12条，恢复后的Flink重放第9～12条时均跳过；下一个更晚的状态使用数据库序号13，覆盖槽位2。此时数据库保留序号4～13。

## 表与字段

手工执行 [建表脚本](../sql/ignite2-heartbeat.sql)，新建 `PUBLIC.HB_STATE`。本次按新项目部署，不迁移旧表数据、不加载旧项目的Checkpoint/Savepoint。新程序只访问HB_STATE，原有两张表不会被自动读取、修改或删除。已有同名HB_STATE表导致脚本报错时，先核对结构。

业务列沿用《心跳数据摸底情况.docx》中“拆分后实际字段”的41项拼写，不添加STATE前缀，也不保留端口数字后缀。建表时字段标识符不加双引号，Ignite统一保存为大写；查询可直接写 `eid`、`floor`、`mainfaultcode`，也可以使用混合大小写。Java解析模型、原始Kafka字段和预览JSON继续使用原有名称。

| 字段 | 含义 |
| --- | --- |
| EId | 文档中的电梯IntId，与SLOT组成主键 |
| SLOT | -1为最新状态；0～N−1为历史状态；运行元数据 |
| CREATED_TIME_MS | CreatedDate按Asia/Shanghai转换的UTC Unix毫秒，唯一新旧比较依据 |
| SendTime | 可选SendTime正整数，沿用既有BIGINT规范化规则，缺失或非法为NULL；不参与时间规则 |
| SEND_TIME_RAW | SendTime字段值的JSON表示，仅追踪用途 |
| DB_SEQUENCE | 数据库成功接受的状态序号 |
| HISTORY_LIMIT | 写入时采用的历史保留上限；SLOT=-1记录该梯当前已应用上限 |
| HISTORY_POLICY_VERSION | 写入时采用的策略版本；固定配置为0，动态配置为正整数 |
| FLINK_SEQUENCE | 解析层输出的historySequence |
| PORT | 原心跳端口1～3 |
| RECEIVED_AT_MS / PARSED_AT_MS / STORED_AT_MS | 接收、解析和写入时间，毫秒 |
| SOURCE_TOPIC / SOURCE_PARTITION / SOURCE_OFFSET | Kafka来源坐标 |
| KAFKA_TIMESTAMP_MS | Kafka记录时间 |
| IntId / FId / Code | 文档中的终端IntId、自增主键和IMSI码 |
| Floor / ACD / Direction / RS / DoorLock 等 | 文档中的单梯状态字段，每个字段独立一列 |
| MainFaultCode / SubFaultCode | 字符串故障码，保留前导零 |
| CreatedDate / UpdatedDate | 原始日期文本 |
| part_day | 按CreatedDate的北京时间日期生成，格式yyyy-MM-dd；普通查询列，不是物理分区配置 |

业务字段使用 `BIGINT` 或 `VARCHAR`，缺失值保持NULL；不补零、不继承旧值。字段与类型的完整清单以SQL脚本为准，`SecDoorCouter`、`TCDOpenDoorRuning` 等名称保留文档既有拼写。文档示例中的空字符串SendTime会保存为NULL，其原始JSON表示保留在SEND_TIME_RAW中，CreatedDate仍然决定新旧、过期和part_day。

缓存名为 `HB_STATE`，类型为 `PARTITIONED + TRANSACTIONAL + FULL_SYNC`，当前脚本 `BACKUPS=0`，使用 `heartbeat` 数据区域。同一EId的最新及历史槽位通过 `AFFINITY_KEY=EID` 放在同一分区。二进制类型为 `HeartbeatStateKeyV2` / `HeartbeatStateRowV2`。启动检查缓存存在性、事务模式、同步模式、数据区域名称、无过期策略、表名、字段及大小写、复合主键及二进制类型。程序只访问已有缓存。

客户端无法仅凭缓存配置证明服务端已开启持久化。`--check-ignite` 的成功只证明连接、ACTIVE状态和上述结构符合要求；服务端持久化和WAL设置由现有服务管理员核实。

## 准备部署文件

将最新工程复制到提交节点，至少包含以下文件。保留实际环境的 `config/local.properties`，不要用示例文件覆盖已有连接配置：

```text
/home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar
/home/xiong/projects/YX2602_FAULTRECOG/config/local.properties
/home/xiong/projects/YX2602_FAULTRECOG/bin/watch-history-config.sh
/home/xiong/projects/YX2602_FAULTRECOG/sql/ignite2-heartbeat.sql
```

若尚未生成JAR，在具备Maven及依赖访问条件的构建机或提交节点执行：

```bash
cd /home/xiong/projects/YX2602_FAULTRECOG
export JAVA_HOME=/opt/java11
export PATH=/opt/java11/bin:$PATH
java -version
mvn -version
mvn -B -ntp clean verify
```

两条版本命令均应显示Java 11。构建完成后确认目标JAR存在；在其他机器构建时，将产物放入上述固定位置。部署已有JAR不需要集群安装Maven。

在所有可能运行AM/JobManager或TaskManager的节点分别执行 `/opt/java11/bin/java -version`，确认同一路径存在且版本为11。只在提交节点安装Java 11无法保证YARN容器能运行本项目。Windows复制过来的shell脚本应保持LF换行。

## 接入已有 Ignite 服务

本项目通过Java thin client连接其他团队维护的Ignite 2.18.0服务。项目部署只需准备业务表、配置连接参数并提交Flink作业；服务端启动、集群激活、内存及磁盘配置由服务管理员负责。

向服务管理员确认以下接入信息：

- 客户端地址、端口（常用10800）、账号密码及TLS要求，确保提交节点和YARN容器能访问。
- 服务处于ACTIVE状态，支持SQL建表及事务缓存，并提供业务表所需的读写权限。
- 业务数据所在区域的持久化、WAL和副本策略，以及是否符合项目对数据恢复的要求。

当前建表脚本指定 `DATA_REGION=heartbeat`、`BACKUPS=0`，代码也校验数据区域名为 `heartbeat`。这些仍是当前实现的要求，需要管理员确认是否适合现有服务；若数据区域或副本策略不同，应先适配脚本和相关校验，不能假定现有服务符合要求。

指定 `DATA_REGION=heartbeat` 不会自动启用持久化。请管理员确认该区域实际的 `persistenceEnabled` 设置，以及重启前后的存储目录、磁盘挂载和节点标识。`BACKUPS=0` 表示没有备份副本，不能提供节点故障切换保障。Flink检查点不会备份Ignite表中的业务数据。

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

保留现有 `config/local.properties` 的Kafka地址、消费组和HDFS目录，将示例配置中的Ignite连接参数、历史条数和共享配置路径添加进去，并填写。以下使用当前选定的动态配置路径：

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
ignite.history.config.path=hdfs:///flink/projectConfig/faultRecog/heartbeat-history.properties
ignite.history.refresh.interval.ms=10000
checkpoint.storage=hdfs:///flink/checkpoints/heartbeat3-reader
checkpoint.interval.ms=30000
checkpoint.timeout.ms=180000
```

`ignite.retry.attempts` 包含首次尝试。可选 `ignite.username` 和 `ignite.password` 必须同时填写，支持既有 `${ENV_NAME}` 替换。SSL启用时，信任库/密钥库沿用Java SSL系统属性，在实际运行客户端的提交进程、JobManager和TaskManager均需可见；不要把密码放到共享命令历史中。

输入Topic和异常Topic须提前创建，并分别授予消费、位点管理和异常写入所需权限。正式作业独占配置的消费组，提交前停止使用同一消费组的预览作业。提交节点及YARN执行节点均需能访问Kafka、Ignite和HDFS；提交账号需有检查点目录写权限，配置同步进程还需要共享配置目录读写权限。

使用 `${ENV_NAME}` 时，提交终端export的变量不会自动成为JobManager容器的环境。应按集群方式提供容器环境，或使用已填写实际值且访问受控的配置文件；不要将密码放入提交命令。

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

需要先只检查配置格式时，将上述命令最后的 `--check-ignite` 改为 `--check-config`。看到“Ignite连接与缓存结构校验通过”后再提交正式作业；预检通过仍不能代替YARN容器内的网络及运行环境检查。

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
hdfs dfs -cat /flink/projectConfig/faultRecog/heartbeat-history.properties
```

同步进程默认每10秒检查原文件，每个写入器默认再每10秒读取一次共享文件；正常情况下发布与读取合计约0～20秒，再等待该梯的下一条新心跳。刷新间隔由启动时的 `ignite.history.refresh.interval.ms` 设置，最小1000毫秒。成功加载后，下一批事务使用新配置快照；各任务的刷新时刻不同，不保证全局同时切换。某梯接受下一条比库内CreatedDate更新的心跳时，才会应用新的保留上限。重复、乱序旧数据或被解析层过滤的数据不会触发清理。

- 调小：例如20→5，下一条新记录与已有最新4条一起保留，删除与写入在同一事务完成。离线电梯暂不清理。
- 调大：例如5→20，保留现有记录，再逐步积累到20条；已删除的数据不会恢复。
- 同步失败：原文件缺少显式条数、格式非法、条数超出1～1000或HDFS发布失败时，保留已发布的有效配置，记录告警并重试。不要在local.properties中填写version。共享路径改变需重启同步进程及重新提交作业；其他启动参数没有热更新能力。
- 刷新失败：共享文件暂时不可读、格式错误或版本倒退时，写入器继续使用最后有效值并记录告警；首次加载失败则启动失败。
- 版本保护：每台梯在SLOT=-1的最新状态记录中保存已应用版本，刷新较慢或重启的任务不能把它改回旧版本。版本由同步程序自动管理。

不配置 `ignite.history.config.path` 时使用固定的 `max.records`（默认10），修改固定值需要重新提交。启用共享文件后，写入器以同步程序发布的值为准。已经应用动态版本的电梯不会因删除路径配置而退回版本0；后续仍应保持同步程序运行，通过修改源文件条数调整。`file:///` 仅适用于本地测试或所有执行节点都能看到同一文件内容的共享挂载，YARN环境优先使用HDFS。同步脚本要求提交节点可执行 `hdfs dfs`、具备目标目录的创建/读写权限，安全集群还需保持Hadoop身份凭据有效。

配置中的HDFS路径是历史条数策略文件的位置，刷新间隔是配置检查频率；二者都不控制心跳读取频率或Ignite写入周期。无需到HDFS手动改文件。更改共享路径后，需要停止旧同步进程，按新路径重新初始化和启动同步进程，并重新提交Flink作业；仅调整 `max.records` 时不需要重启。

## YARN Application正式运行

完成前述建表、配置和动态配置同步准备后，可直接使用启动脚本（与下方提交命令参数一致）：

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/start-heartbeat.sh
```

脚本使用固定集群路径，检查所需文件和可执行程序，加载CDH环境后提交作业；无需重新打包JAR。每次执行都会提交一个新的Application，不会停止或复用已有作业，已有任务运行时不要重复执行。历史配置同步仍由 `watch-history-config.sh` 单独负责。脚本为Linux LF换行，请使用Bash执行。

所有可能运行JobManager/TaskManager的节点均须安装Java 11到 `/opt/java11`。提交终端的JAVA_HOME和 `env.java.home` 不能替代YARN容器的JAVA_HOME；下方两个 `containerized.*.env.JAVA_HOME` 参数分别指定AM/JobManager和TaskManager的Java路径。保持集群现有Kafka父加载优先参数，其客户端兼容性仍需集群验证。

```bash
source /opt/flink-1.20.5/conf/cdh-env.sh
export JAVA_HOME=/opt/java11
export PATH=/opt/java11/bin:$PATH

hdfs dfs -mkdir -p /flink/checkpoints/heartbeat3-reader /flink/savepoints/heartbeat3-reader

/opt/flink-1.20.5/bin/flink run-application \
  -t yarn-application \
  -Denv.java.home=/opt/java11 \
  -Dcontainerized.master.env.JAVA_HOME=/opt/java11 \
  -Dcontainerized.taskmanager.env.JAVA_HOME=/opt/java11 \
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

本次单表结构按新项目首次部署，先停止旧作业并创建HB_STATE，再提交新版JAR，不携带旧项目的Checkpoint/Savepoint。若需要从部署时的最新Kafka位置开始，使用新的消费组；沿用已有消费组会从其已提交位点开始，超过10分钟的数据仍会过滤。后续恢复本单表项目时，在上述JAR路径之前添加 `-s 实际保存点路径`。CreatedDate状态名为 `last-accepted-created-version-v1`，写入算子UID为 `heartbeat-ignite-sink-v1`。保留原 `parser.max.parallelism`。不要同时运行两个使用同一消费组的作业。

预览消费过的正常消息没有入库。切换正式模式后，从恢复位点继续处理；不会自动补写预览期间的数据，也不会回填超过10分钟的历史。需要已有状态时，应等待设备上报新的心跳。

## 运行验收与日志

记录提交返回的Application ID，通过YARN Tracking URL打开Flink Web UI。以下ID变量需要替换为本次实际值：

```bash
export FLINK_APP_ID='application_替换为实际ID'
yarn application -status "$FLINK_APP_ID"
/opt/flink-1.20.5/bin/flink list \
  -t yarn-application -Dyarn.application.id="$FLINK_APP_ID"
yarn logs -applicationId "$FLINK_APP_ID" \
  > /home/xiong/projects/YX2602_FAULTRECOG/heartbeat-yarn.log 2>&1
```

确认以下结果后再判定部署成功：

- Flink作业为RUNNING，所需TaskManager已注册，容器实际使用Java 11。
- Web UI的Checkpoints页面持续出现成功检查点，没有持续恢复或积压。
- [业务查询](#检查点重试与运维) 中的最新状态和历史随有效新心跳更新；历史条数符合该梯已应用策略。
- 配置同步日志没有持续错误，HDFS共享文件可读取；`igniteHistoryConfigRefreshFailed` 为0。

正式模式的正常心跳写入Ignite，不会像 `--preview` 一样逐条打印JSON。收到Application ID仅表示提交已受理。启动错误优先看JobManager/AM的 `stderr`、`jobmanager.log` 和 `prelaunch.err`，后续处理错误还需查看TaskManager日志。未启用日志聚合或日志尚未聚合时，从YARN页面进入对应Attempt和容器的Logs。

## Ignite实际持久化目录

2026-11-04 根据服务器 `AI-slave06` 的现场命令截图核对：该节点的Ignite运行在Docker容器环境中，`HB_STATE` 分区数据文件位于容器的 `/opt/ignite/work/db`；对应宿主机目录为 `/home/workdir/ignite2/standalone/ignite-data/db`，底层磁盘为挂载在 `/home` 的 `/dev/sdb1`（XFS）。以下记录只对应本次核查节点，不代表其他Ignite节点使用相同目录。

### 目录与磁盘对照

| 内容 | 已核对的位置或信息 |
| --- | --- |
| 宿主机 | `AI-slave06` |
| 宿主机挂载给容器的目录 | `/home/workdir/ignite2/standalone/ignite-data` |
| 容器内对应目录 | `/opt/ignite/work` |
| 宿主机持久化数据目录 | `/home/workdir/ignite2/standalone/ignite-data/db` |
| 容器内持久化数据目录 | `/opt/ignite/work/db` |
| 本次节点数据子目录 | `node00-e7e19ede-ebc1-440d-92f2-915c019ad86f` |
| 心跳表及缓存名 | `PUBLIC.HB_STATE` / `HB_STATE` |
| 心跳缓存文件夹 | 节点数据子目录下的 `cache-HB_STATE` |
| 底层磁盘 / 文件系统 | `/dev/sdb1` / XFS |
| 宿主机磁盘挂载点 | `/home` |
| 核查时磁盘容量 / 已使用 | 约59 TB / 114 GB；这是整个文件系统的统计，不是Ignite单独占用量 |

本次 `HB_STATE` 在宿主机上的完整目录：

```text
/home/workdir/ignite2/standalone/ignite-data/db/node00-e7e19ede-ebc1-440d-92f2-915c019ad86f/cache-HB_STATE/
```

该目录中的 `part-*.bin` 是分区数据文件，不是一条心跳一个文件。每梯历史保留上限限制的是逻辑记录数，不意味着这些文件的数量或磁盘占用会随每次覆盖、删除立即减少。

### 现场确认依据与待核实项

- 当时Ignite进程PID为 `114905`，工作目录为 `/opt/ignite`。PID仅用于记录本次取证，进程重启后需重新查找。
- `/proc/114905/fd/*` 的链接显示运行进程实际打开了 `/opt/ignite/work/db/node00-e7e19ede-ebc1-440d-92f2-915c019ad86f/cache-HB_STATE/part-*.bin`。
- `df -hT /proc/114905/root/opt/ignite/work/db` 显示底层文件系统为 `/dev/sdb1`、XFS，宿主机挂载点为 `/home`。
- `/proc/114905/mountinfo` 中根文件系统为Docker overlay；数据挂载行的文件系统内源目录是 `/workdir/ignite2/standalone/ignite-data`，容器挂载点是 `/opt/ignite/work`，设备是 `/dev/sdb1`。结合宿主机 `/home` 挂载点，得到上述宿主机完整路径。

宿主机直接访问 `/opt/ignite/work/db` 曾提示不存在，这是容器内路径与宿主机路径不同所致。宿主机运维应使用 `/home/workdir/ignite2/standalone/ignite-data`。

启动参数中的配置名称为 `config/default-config.xml`；宿主机曾搜索到 `/home/lwh/apache_ignite/ignite-2.18.0/config/default-config.xml`，但尚未证实它就是运行进程加载的配置。WAL、WAL归档的实际目录及 `heartbeat` 区域的服务端配置内容仍未核实，不将默认目录或历史模板当作现场配置。已打开的数据文件及挂载信息确认的是本节点当前的数据落盘位置，不替代重启恢复验证。

### 下次直接查看

在宿主机 `AI-slave06` 执行以下只读命令：

```bash
# 确认目录及所在磁盘。
ls -ld /home/workdir/ignite2/standalone/ignite-data/db
findmnt -T /home/workdir/ignite2/standalone/ignite-data/db
df -hT /home/workdir/ignite2/standalone/ignite-data/db

# 查看整个Ignite挂载目录的磁盘占用。
du -sh /home/workdir/ignite2/standalone/ignite-data

# 查找各节点子目录中的心跳缓存文件夹，不依赖旧的节点目录名称。
find /home/workdir/ignite2/standalone/ignite-data/db \
  -maxdepth 2 -type d -name 'cache-HB_STATE'

# 需要重新核对运行进程时，先查询当前PID。
ps -ww -eo pid,args | grep '[o]rg.apache.ignite'
```

容器重建、挂载调整或节点身份变化后，应重新核对并更新本节。项目中的 `local.properties` 和Flink启动脚本不设置上述服务端存储目录；HDFS历史策略文件、Checkpoint与Savepoint的位置见下节。

## Checkpoint与Savepoint

| 内容 | 位置与生成方式 |
| --- | --- |
| Checkpoint | `local.properties` 的 `checkpoint.storage` 决定；当前为 `hdfs:///flink/checkpoints/heartbeat3-reader`，默认每30秒触发 |
| Savepoint | 代码没有固定默认目录，手动触发时指定；本部署约定使用 `hdfs:///flink/savepoints/heartbeat3-reader` |
| 历史条数动态配置 | 当前为 `hdfs:///flink/projectConfig/faultRecog/heartbeat-history.properties`，只保存策略，不保存Flink状态或心跳数据 |
| 心跳业务数据 | 存放在Ignite的 `PUBLIC.HB_STATE`；已核对节点的宿主机数据目录为 `/home/workdir/ignite2/standalone/ignite-data/db`，详见 [Ignite实际持久化目录](#ignite实际持久化目录) |

Checkpoint目录通常包含 `<JobID>/chk-<编号>/`，具体完成检查点的恢复路径以Web UI中Checkpoints页面的External Path为准。代码设置取消后保留外部检查点；这不表示永久保留全部历史检查点，也不表示重新提交时会自动选中旧检查点。修改 `checkpoint.storage` 需要重新提交，运行中的作业使用提交时加载的值。

仅创建Savepoint目录不会产生保存点。准备停止作业维护时，使用本次Application ID和Flink JobID执行：

```bash
export FLINK_JOB_ID='替换为实际FlinkJobID'
/opt/flink-1.20.5/bin/flink stop \
  -t yarn-application -Dyarn.application.id="$FLINK_APP_ID" \
  --savepointPath hdfs:///flink/savepoints/heartbeat3-reader \
  "$FLINK_JOB_ID"
```

此命令生成保存点并停止作业，当前单作业Application随后退出。若只需生成保存点而继续运行，使用：

```bash
/opt/flink-1.20.5/bin/flink savepoint \
  -t yarn-application -Dyarn.application.id="$FLINK_APP_ID" \
  "$FLINK_JOB_ID" hdfs:///flink/savepoints/heartbeat3-reader
```

记录命令实际返回的完整Savepoint路径，例如 `hdfs:///flink/savepoints/heartbeat3-reader/savepoint-...`。恢复时，在上方完整 `run-application` 命令的JAR路径之前增加 `-s 实际完整Savepoint路径`，保留Java 11容器参数、配置分发参数、消费组、算子UID和兼容状态配置。不要将Savepoint父目录直接作为恢复路径，也不要在旧作业仍运行时用同一消费组启动恢复作业。

作业内故障恢复使用最近完成检查点；人工重新提交时应显式选择兼容的保存点或外部检查点。没有恢复Flink状态时，读取入口会从Kafka消费组已有提交位点继续；无提交位点的分区初始化到当时末尾。Flink状态恢复不会重建丢失的Ignite表或自动回填全部业务历史，超过10分钟的数据仍按业务规则过滤。

## 常见部署问题

| 日志或现象 | 检查与处理 |
| --- | --- |
| `UnsupportedClassVersionError`，class file version 55.0、runtime最多支持52.0 | JAR由Java 11编译，失败容器实际用了Java 8。检查两个 `containerized.*.env.JAVA_HOME` 参数，并在执行节点运行 `/opt/java11/bin/java -version` |
| `/opt/java11/bin/java` 不存在或无执行权限 | 在实际承载容器的节点核对Java安装路径和权限；提交节点验证通过不能代替执行节点检查 |
| 无法加载历史动态配置、HDFS权限或连接错误 | 核对本地共享路径、同步进程日志、Hadoop身份和容器HDFS环境，确认已成功执行一次初始化 |
| 缺少 `HB_STATE` 或结构不匹配 | 由管理员核对实际连接集群、Schema和当前SQL结构（含业务列大小写及复合主键）；已有表先检查 |
| Ignite节点重启后表或数据不可见 | 请管理员检查集群成员、`heartbeat` 区域持久化设置、原数据目录和挂载；客户端预检无法证明持久化已开启 |
| SLF4J重复绑定警告，随后AM容器退出 | 警告本身不足以定位退出原因，继续查同一失败容器最底层异常和启动日志 |

AM启动失败时先保存本次应用日志，再提取异常上下文：

```bash
grep -nE -B 8 -A 20 \
  'Caused by:|Exception|Error:|Unrecognized|No such file|Permission denied|JAVA_HOME|prelaunch' \
  /home/xiong/projects/YX2602_FAULTRECOG/heartbeat-yarn.log
```

上述命令依据当前入口和配置编写；实际部署仍需按本节验收。示例内存和并行度用于初始联调，生产容量应结合真实负载、Kafka分区数和集群资源评估。

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
SELECT eid, createddate, DB_SEQUENCE, floor, mainfaultcode, part_day
FROM PUBLIC.HB_STATE WHERE SLOT = -1 ORDER BY eid LIMIT 20;

SELECT eid, SLOT, createddate, floor, mainfaultcode, DB_SEQUENCE, FLINK_SEQUENCE
FROM PUBLIC.HB_STATE WHERE eid = 12345 AND SLOT >= 0 ORDER BY DB_SEQUENCE;

SELECT eid, HISTORY_LIMIT, HISTORY_POLICY_VERSION
FROM PUBLIC.HB_STATE WHERE SLOT = -1 ORDER BY eid LIMIT 20;

SELECT H.eid, COUNT(*) AS HISTORY_COUNT, L.HISTORY_LIMIT
FROM PUBLIC.HB_STATE H JOIN PUBLIC.HB_STATE L ON H.eid=L.eid AND L.SLOT=-1
WHERE H.SLOT >= 0
GROUP BY H.eid, L.HISTORY_LIMIT HAVING COUNT(*) > L.HISTORY_LIMIT;
```

将12345替换为真实电梯ID，最后一个查询在写入稳定时应返回空结果。查询最新状态必须筛选SLOT=-1，查询或统计历史必须筛选SLOT>=0，避免把最新状态重复计入历史。环形槽位不是时间排序，查询历史必须按DB_SEQUENCE或CREATED_TIME_MS排序。SLOT=-1行的策略字段代表该梯实际已应用配置；子任务指标仅代表已加载目标，离线电梯可能仍使用旧版本。重排时保留的历史行携带原写入时的策略字段，不能用这些旧历史字段判断当前上限。

## 验证范围

时间规则及验证说明见 [CreatedDate说明](heartbeat-time.md)。

单表版本在2026-11-04以Java 11完成 `mvn clean verify`，152项测试通过。业务列名另与原始心跳文档的拆分后字段清单逐项核对，41项字段拼写一致（数据库按无引号规则转大写）。三个端口均验证全部41个字段在小写、混合大小写和大写无引号SQL下的查询值、故障码前导零、最新与历史槽位内容一致，以及CreatedDate北京时间跨日时的part_day。

本地测试包含真实Ignite 2.18.0持久化节点、SQL建表、Java thin client和Flink MiniCluster。覆盖空值/前导零、循环历史、数据库与Flink序号分离、重放过滤、同梯并发首次写入、批次中途失败回滚、持久化重启、检查点提交等待、数据库中断后的检查点失败与恢复、队列背压及正常结束刷新。动态配置通过临时本地文件验证后台热加载、10→20→5→1→4扩缩容、离线延迟清理、无效配置保留旧值、旧任务版本保护，以及历史重排与策略元数据的事务回滚。同步程序验证只修改local.properties即可发布并被写入器读取、版本自动递增及重启延续、拒绝重复同步进程、缺少条数或无效配置不覆盖有效文件、生成文件不包含凭据，以及最终JAR独立运行配置发布入口。

HDFS动态文件读取及权限、内网Kafka、YARN容器网络、集群类加载配置、服务端进程崩溃/断电，以及10万条/秒吞吐量仍需集群验收。批次事务涉及多次网络调用，调整上限时还需重排该梯历史；当前默认资源与批大小是联调配置，不能据此承诺生产吞吐量。
