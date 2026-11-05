# 心跳状态写入 Ignite 2.18.0

本模块将解析层接受的单梯状态写入 Ignite。Java 编译和运行版本统一为11，Flink保持1.20.5，Ignite客户端和服务端均为2.18.0。业务时间统一使用CreatedDate（北京时间），读取、解析算子UID保持不变。时间规则见 [CreatedDate说明](heartbeat-time.md)。

当前部署使用独立 `heartbeat_memory` 区域，服务端须设置 `persistenceEnabled=false`。全停后允许丢失窗口，随新心跳重建，不做历史回放。**本次切换与全停后的启动以 [内存模式](ignite-memory.md) 为准**；下文保存点命令用于需要保留Flink处理进度的维护，不用于恢复丢失的Ignite数据。历史性能测试与磁盘目录记录标注的是旧持久化方案。

旧表升级到无需引号的字段结构，请按 [清空重建与重新部署](redeploy-unquoted-columns.md) 操作。必须同时更新建表脚本和JAR，不能只改SQL文件。

## 正式部署顺序

以下步骤在集群提交节点执行，项目固定放在 `/home/xiong/projects/YX2602_FAULTRECOG`。本项目连接已有Ignite服务，服务端由管理员维护。

1. [准备部署文件](#准备部署文件)，确认所有可能运行YARN容器的节点均有Java 11。
2. [接入已有Ignite服务](#接入已有-ignite-服务)，由有权限的人员核对并创建业务表。
3. [填写配置](#配置与预检)，检查Kafka Topic、消费组、Ignite连接参数和HDFS目录。
4. [初始化并启动历史配置同步](#历史条数热更新)，成功生成共享文件后执行配置与数据库预检。
5. [提交YARN Application](#yarn-application正式运行)，正式模式使用 `--run`。
6. [检查作业和入库结果](#运行验收与日志)，确认RUNNING、检查点成功、业务数据持续更新。
7. 内存服务全停后按 [内存模式](ignite-memory.md) 直接启动新窗口；仅需要保留Flink处理进度的维护可参考 [Checkpoint与Savepoint](#checkpoint与savepoint)。

## 写入契约

所有状态存入一张 `PUBLIC.HB_STATE`，复合主键为 `(EID, SLOT)`。每台电梯的 `SLOT=-1` 固定保存最新状态，非负槽位保存最近N条历史；N默认10，可在1～1000之间配置并在运行中调整。历史包含当前最新状态，因此每梯最多N+1行；累计不足N条时不填充空行。保留的是成功接受的最近N次状态，不是最近10分钟的全部数据。调小上限后，各电梯在下一条新心跳被接受时清理；离线电梯暂时维持原有历史。

事务使用 Ignite Java thin client 的缓存 API，模式为 `PESSIMISTIC + REPEATABLE_READ`。SQL用于缺表时自动建表（也可手工执行）与查询。每批事务按电梯ID排序获取锁，同一电梯的输入顺序保持不变：

1. 事务内按电梯ID顺序读取并锁定该梯 `SLOT=-1` 的最新状态，包括尚不存在的复合主键；每批每梯只读取一次，两个并发首次写入也按这个固定键串行处理。
2. `createdTimeMs <= 数据库最新CREATED_TIME_MS` 时忽略，不递增序号，不写历史。
3. 新记录的 `DB_SEQUENCE = 旧值 + 1`，首条为1。
4. 根据配置版本和该梯已应用版本确定N；N变化时，在事务内取出旧历史、保留最近的至多N−1条，并按新N重排槽位，为新记录预留一格。
5. 历史槽位 `SLOT = (DB_SEQUENCE - 1) % N`，范围0～N−1。同一快照暂存为 `SLOT=-1` 和对应历史槽位的新值。同梯后续记录使用批内最新状态逐条判定，序号逐条递增；批末将全部槽位最终值通过一次 `putAll` 写入，连同策略版本、历史重排一起提交；任何异常都回滚。调整历史时只操作非负槽位，不能删除最新状态的固定槽位。

数据库序号与 Flink 的 `historySequence` 分开：后者保存为 `FLINK_SEQUENCE`，用于追踪。数据库可能已提交，但对应Flink检查点未完成；重放时依靠数据库CreatedDate时间过滤，不能使用恢复后的Flink序号覆盖历史槽位。

例如N=10时，数据库已经写到第12条，恢复后的Flink重放第9～12条时均跳过；下一个更晚的状态使用数据库序号13，覆盖槽位2。此时数据库保留序号4～13。

## 写入吞吐优化与验收

周期性日志、各阶段计时和定位示例见 [性能日志与积压定位](heartbeat-monitoring.md)。默认 `monitor.log.interval.ms=30000`，在TaskManager日志搜索 `[HEARTBEAT-PERF]` 和 `[HEARTBEAT-CHECKPOINT]`。

2026-11-05优化客户端批内写入：保持按电梯ID顺序读取并持有事务锁，每台电梯在一批内只远程读取一次；最新状态与历史槽位先在批内计算，最终通过一次 `putAll` 提交到缓存。相同时间及旧状态仍跳过；每条接受记录均递增序号，历史最终内容与原逐条写入一致。批内对相同槽位的中间覆盖不再逐次发送到服务器。

| 正常批次场景 | 优化前Cache API调用 | 优化后Cache API调用 |
| --- | --- | --- |
| 128台不同电梯各1条新心跳 | 128次get + 256次put，共384次 | 128次get + 1次putAll，共129次 |
| 同一电梯连续128条新心跳 | 128次get + 256次put，共384次 | 1次get + 1次putAll，共2次 |

统计不含事务开始/提交、连接与元数据请求、重试及历史上限调整；是客户端API调用次数，不是磁盘I/O次数或吞吐倍数。大量不同电梯的业务数据仍需落盘，这次改动不能保证解除服务端存储瓶颈。按默认批大小128，最终待写入集合最多256个槽位；正常事务仍为 `PESSIMISTIC + REPEATABLE_READ`，读取的固定键锁保持到批量写入提交结束。

新增运行指标：`igniteLastBatchDurationMs` 查看最近成功批次的写入耗时，`igniteLastBatchSize` 查看实际批大小。两次采样的 `igniteWriteTimeMs` 增量除以 `igniteCompletedBatches` 增量，得到该窗口平均批次耗时（分母非零时）；`igniteCommittedRows` 增量除以采样秒数，得到接受的单梯心跳数/秒。批次耗时含事务执行、连接重试等 `store.write` 时间，不含前面的排队及凑批时间。最后成功返回时间由 `igniteLastCommitAtMs` 记录；没有输入时该时间不变属于正常现象。

### 更新与效果对比

本节用于同一存储模式下的性能版本更新。旧持久化区域首次切换到内存区域时，请使用 [内存模式部署说明](ignite-memory.md)，其中需要处理旧同名表，不能直接套用本节的保存点升级步骤。

这次优化需要更新JAR；表结构、配置键、算子UID和Flink状态结构沿用当前单表版本，可以从本单表作业Savepoint恢复。操作顺序：

1. 先记录当前消费组、并行度、两个相隔60秒的入库指标、队列、Checkpoint耗时及各分区LAG；确认比较的单位为单梯心跳，Kafka消息可能拆成多梯。
2. 按 [Checkpoint与Savepoint](#checkpoint与savepoint) 执行停止并生成Savepoint，等待成功，记录完整路径并确认旧作业停止。
3. 备份服务器旧JAR，再将新 `target/heartbeat-reader-0.1.0.jar` 与其 `.sha256` 文件复制到 `/home/xiong/projects/YX2602_FAULTRECOG/target/`。如果启动脚本尚未更新，一并更新 `bin/start-heartbeat.sh`。不要执行重建表操作，不要用示例或本地配置覆盖服务器的 `local.properties`。
4. 在服务器校验后，从步骤2保存点恢复：

```bash
cd /home/xiong/projects/YX2602_FAULTRECOG/target
sha256sum -c heartbeat-reader-0.1.0.jar.sha256

bash /home/xiong/projects/YX2602_FAULTRECOG/bin/start-heartbeat.sh \
  --from-savepoint '替换为本次完整Savepoint路径'
```

验收时保持原消费组、并行度、批大小和内存，先比较这次代码更新的效果。确认Checkpoint持续成功，窗口入库吞吐是否提高、队列是否下降，并比较相同Checkpoint提交阶段的LAG及最近活跃电梯的端到端时间差。稳定的已提交LAG可能包含检查点提交周期带来的差距，不能直接当作所有消息都未读取。若磁盘仍长时间排队且实际数据延迟超标，需要Ignite服务管理员继续排查WAL及数据盘、共享I/O竞争；新增客户端指标不能证明磁盘上限。

2026-11-05在Java 11执行离线 `mvn -o -B -ntp clean verify`，157项测试全部通过，无失败、错误或跳过。本地验证覆盖真实持久化Ignite的批量写入、同梯批内重复与乱序、循环历史、扩缩容、已发送到服务端后失败的事务回滚、并发重叠批次，以及最终打包JAR独立进程运行；集群吞吐改善需按上述步骤验收。保留原有CreatedDate过期过滤规则，超过10分钟的积压仍会被过滤。

## 表与字段

正式作业在表不存在时执行JAR内的 [建表脚本](../sql/ignite2-heartbeat.sql)，新建空 `PUBLIC.HB_STATE`；也可由管理员手工执行。本次按新项目部署，不迁移旧表数据、不加载旧项目的Checkpoint/Savepoint。已有同名表会严格校验，不自动删除、修改或迁移；IF NOT EXISTS不会改变旧表所在区域。

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
| RECEIVED_AT_MS / PARSED_AT_MS / STORED_AT_MS | 接收、解析阶段和构造入库内容的时间，毫秒；STORED_AT_MS不是精确的事务提交完成时间 |
| SOURCE_TOPIC / SOURCE_PARTITION / SOURCE_OFFSET | Kafka来源坐标 |
| KAFKA_TIMESTAMP_MS | Kafka记录时间 |
| IntId / FId / Code | 文档中的终端IntId、自增主键和IMSI码 |
| Floor / ACD / Direction / RS / DoorLock 等 | 文档中的单梯状态字段，每个字段独立一列 |
| MainFaultCode / SubFaultCode | 字符串故障码，保留前导零 |
| CreatedDate / UpdatedDate | 原始日期文本 |
| part_day | 按CreatedDate的北京时间日期生成，格式yyyy-MM-dd；普通查询列，不是物理分区配置 |

业务字段使用 `BIGINT` 或 `VARCHAR`，缺失值保持NULL；不补零、不继承旧值。字段与类型的完整清单以SQL脚本为准，`SecDoorCouter`、`TCDOpenDoorRuning` 等名称保留文档既有拼写。文档示例中的空字符串SendTime会保存为NULL，其原始JSON表示保留在SEND_TIME_RAW中，CreatedDate仍然决定新旧、过期和part_day。

缓存名为 `HB_STATE`，类型为 `PARTITIONED + TRANSACTIONAL + FULL_SYNC`，当前脚本 `BACKUPS=0`，使用 `heartbeat_memory` 数据区域。同一EId的最新及历史槽位通过 `AFFINITY_KEY=EID` 放在同一分区。二进制类型为 `HeartbeatStateKeyV2` / `HeartbeatStateRowV2`。启动检查事务模式、同步模式、数据区域名称、无过期策略、表名、字段及大小写、复合主键及二进制类型。正式连接可建缺失空表，只读预检不建表。

客户端无法仅凭缓存配置证明服务端已关闭持久化。`--check-ignite` 的成功只证明连接、ACTIVE状态和上述结构符合要求；服务管理员须确认 `heartbeat_memory.persistenceEnabled=false`。

### 新增辅助字段说明

除文档定义的41个心跳业务字段外，`HB_STATE` 另有16个辅助字段，用于管理最新状态与有限历史、处理时间、追踪处理过程和Kafka来源。这些字段由程序生成或从消息元数据中提取，不代表新增的电梯状态。

#### 最新状态与历史管理（4个字段）

| 字段 | 类型 | 含义与用途 |
| --- | --- | --- |
| `SLOT` | INT | 存储槽位，与业务字段 `EID` 组成主键。`-1`固定保存最新状态；`0～N−1`保存最近N条历史。槽位会循环使用，不是时间顺序或累计记录数。 |
| `DB_SEQUENCE` | BIGINT | 数据库为每台电梯成功接受的新状态递增的序号，从1开始；重复或旧状态不递增。用于历史排序和计算覆盖槽位，不是全表统一序号，也不是当前保存行数。 |
| `HISTORY_LIMIT` | INT | 本条状态写入时采用的历史保留上限N，例如10。`SLOT=-1`记录该梯当前已应用的上限；历史行保留写入当时的值。 |
| `HISTORY_POLICY_VERSION` | BIGINT | 本条状态采用的历史策略版本。固定配置为0，动态配置使用递增的正整数版本，防止刷新较慢的任务用旧配置覆盖新配置；不是心跳数据版本或数量。 |

例如某台电梯已经接受到第12条新状态，保留上限为10：

```text
最新状态：SLOT=-1，DB_SEQUENCE=12
历史状态：SLOT=0～9，合计保存DB_SEQUENCE=3～12的10条状态
本次历史写入槽位：(12-1)%10=1
```

最新状态也包含在历史中，因此该梯最多占11行，其中两行包含第12条的业务状态。`DB_SEQUENCE=12`不表示当前保存12条。查询历史按 `DB_SEQUENCE` 或 `CREATED_TIME_MS` 排序，不按SLOT排序。调小N后，在该梯下一条有效新心跳到达时清理，离线电梯暂时保留旧上限；判断实际生效策略应查看 `SLOT=-1`。

#### 业务时间辅助信息（3个字段）

| 字段 | 类型 | 含义与用途 |
| --- | --- | --- |
| `CREATED_TIME_MS` | BIGINT | 将原始 `CreatedDate` 按Asia/Shanghai解释后转换成Unix毫秒时间戳，用于比较新旧、判断重复及过期。与CreatedDate表达同一业务时刻，不是入库时间。 |
| `PART_DAY` | VARCHAR | 从CreatedDate提取的北京时间日期，格式 `yyyy-MM-dd`，方便按日期筛选；是普通查询列，不会自动按天分区、建表或删除数据。 |
| `SEND_TIME_RAW` | VARCHAR | 原始SendTime字段值的JSON表示，保留其原始类型与内容以便排查格式问题；不是整条原始心跳JSON，也不参与新旧或过期判断。 |

例如 `CREATEDDATE='2026/11/4 10:30:00'` 时，`PART_DAY='2026-11-04'`，`CREATED_TIME_MS`是同一北京时间对应的毫秒时间戳。业务时间始终以CreatedDate为准，不以SendTime或Kafka时间替代。

#### 处理过程追踪（5个字段）

| 字段 | 类型 | 含义与用途 |
| --- | --- | --- |
| `PORT` | INT | 单梯状态从原始心跳的第几个电梯端口拆出，取值1～3；不是网络连接端口号。 |
| `FLINK_SEQUENCE` | BIGINT | Flink解析及排序层为该电梯接受的状态生成的 `historySequence`，用于追踪流处理过程。 |
| `RECEIVED_AT_MS` | BIGINT | 程序接收消息时记录的Unix毫秒时间戳，不是设备产生心跳的业务时间。 |
| `PARSED_AT_MS` | BIGINT | 解析、按电梯排序处理阶段接受该状态时记录的Unix毫秒时间戳。 |
| `STORED_AT_MS` | BIGINT | 写入器构造本次数据库记录时记录的Unix毫秒时间戳，发生在事务提交之前，不是精确的提交完成时间。 |

`FLINK_SEQUENCE`与 `DB_SEQUENCE` 分别由Flink状态和数据库管理，均按电梯计数。重新部署或恢复后两者可能不同，不应要求二者始终相等；数据库历史排序和槽位计算使用 `DB_SEQUENCE`。

处理时间差可以辅助定位延迟，但不是纯计算耗时：例如 `PARSED_AT_MS-RECEIVED_AT_MS`可能包含上游排队及网络传输，`STORED_AT_MS-PARSED_AT_MS`可能包含队列等待和凑批时间。后者不包含后续完整事务提交耗时，跨机器比较也依赖时钟同步。

#### Kafka来源追踪（4个字段）

| 字段 | 类型 | 含义与用途 |
| --- | --- | --- |
| `SOURCE_TOPIC` | VARCHAR | 原始消息来自哪个Kafka Topic。 |
| `SOURCE_PARTITION` | INT | 原始消息所在的Kafka分区号，例如2。 |
| `SOURCE_OFFSET` | BIGINT | 原始消息在该分区中的偏移量，例如158963；只在对应Topic和分区内有意义，不是电梯心跳序号。 |
| `KAFKA_TIMESTAMP_MS` | BIGINT | Kafka记录携带的毫秒时间戳；依Kafka配置可能表示生产时间或Broker追加时间，不能直接当成设备业务时间。 |

`SOURCE_TOPIC + SOURCE_PARTITION + SOURCE_OFFSET`用于定位原始Kafka消息；能否回查取决于消息是否仍在Kafka保留期内。同一条原始消息可拆出多台电梯，因此多个状态可能具有相同来源坐标，但EID、PORT不同。

#### 日常查询重点

查看电梯当前状态主要关注EID、CreatedDate、楼层、方向、门锁和故障码等业务字段；检查历史保留是否正常，重点关注SLOT、DB_SEQUENCE、HISTORY_LIMIT。EID是电梯编号，INTID是终端编号，两者在当前映射中含义不同。辅助字段是当前存储实现的一部分，不应仅因不在业务页面展示就直接删列。

```sql
-- 各梯最新状态及当前已应用的保留策略。
SELECT EID, CREATEDDATE, DB_SEQUENCE, HISTORY_LIMIT, HISTORY_POLICY_VERSION
FROM PUBLIC.HB_STATE WHERE SLOT=-1 ORDER BY EID LIMIT 20;

-- 将12345替换成实际电梯编号，查看历史及其Kafka来源。
SELECT EID, SLOT, DB_SEQUENCE, CREATEDDATE, SOURCE_TOPIC, SOURCE_PARTITION, SOURCE_OFFSET
FROM PUBLIC.HB_STATE WHERE EID=12345 AND SLOT>=0 ORDER BY DB_SEQUENCE;
```

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
- 独立 `heartbeat_memory` 区域的 `persistenceEnabled=false`、内存容量及禁用淘汰设置；本项目不要求全停后的数据恢复。

当前建表脚本指定 `DATA_REGION=heartbeat_memory`、`BACKUPS=0`，代码也校验区域名。配置片段、旧表切换和权限要求见 [内存模式部署说明](ignite-memory.md)。

区域名称本身不控制持久化，必须确认服务端实际开关。`BACKUPS=0` 表示没有备份副本，多节点服务中单节点丢失也可能丢失分区。Flink检查点不会备份Ignite表中的业务数据。

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

## 跳过积压从最新消息重新启动

已决定放弃当前未消费消息、从新心跳重新观察时，使用从未用过的新消费组，且不从Checkpoint/Savepoint恢复。此操作跳过初始化位点之前的所有消息，包括最近10分钟内的未消费消息；它不删除Kafka消息，不清除旧消费组的LAG，也不清空Ignite数据。

1. 在当前Flink作业页面点击Cancel Job，等待旧作业停止；也可由运维按实际Application ID停止旧Application。确认旧作业不会继续运行后再提交新作业。
2. 编辑服务器 `/home/xiong/projects/YX2602_FAULTRECOG/config/local.properties`，只修改消费组，例如 `kafka.group.id=heartbeat3-reader-realtime-20261105-01`。该名称只作为本次新组示例，如果已经使用过，换一个未使用的后缀。服务器当前的 `parallelism` 及其余配置保持原值，不要用本地文件整体覆盖服务器配置。
3. 不带恢复参数执行：

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/start-heartbeat.sh
```

代码会为新消费组缺失的各分区位点初始化到启动初始化时的末尾，之后消费新增消息。不能携带 `--from-savepoint` 或 `-s`，否则恢复的Source状态会优先于新组的起始位点。无需重新打包JAR，也无需删除原Checkpoint/Savepoint目录。

Ignite已有最新状态和历史保留，后续按CreatedDate继续更新，`DB_SEQUENCE` 继续以库内值递增；Flink每梯状态及 `FLINK_SEQUENCE` 从新作业重新建立。验收时确认新Application为RUNNING、Checkpoint持续成功，并查看新消费组的LAG；旧消费组的积压仍然存在，不代表新作业没生效。等新组完成成功Checkpoint及Kafka位点提交后，再比较各分区LAG的变化。

本次只重置消费起点，不代表写入吞吐提高。观察新的Sink队列和持续输入下的LAG；如果新组仍持续积压，继续排查写入性能。为判断这次重新起点的效果，建议暂时保持现有并行度及内存设置，避免同时改变多个参数。

## 保留积压调整并行度

本节将已有单表作业的 `parallelism=2` 调整为3，并保留Kafka积压、Flink每梯状态及Ignite已有数据。使用当前正在运行的同一JAR、消费组 `heartbeat3-reader`、算子UID和原来的 `parser.max.parallelism`（未配置时默认128）。这次只修改提交配置和启动脚本，不需要重新打包JAR。先将更新的 `bin/start-heartbeat.sh` 复制到服务器同一位置，该脚本已支持 `--from-savepoint PATH`（也支持 `-s PATH`）。

### 1. 停止旧作业并生成保存点

在提交服务器加载环境，使用当前运行作业的Application ID和JobID。截图中的旧ID只用于定位，执行前以当前YARN/Flink页面为准：

```bash
source /opt/flink-1.20.5/conf/cdh-env.sh
export JAVA_HOME=/opt/java11
export PATH=/opt/java11/bin:$PATH

yarn application -list -appStates RUNNING

export FLINK_APP_ID='application_替换为实际ID'
export FLINK_JOB_ID='替换为当前FlinkJobID'

hdfs dfs -mkdir -p /flink/savepoints/heartbeat3-reader

/opt/flink-1.20.5/bin/flink stop \
  -t yarn-application \
  -Dyarn.application.id="$FLINK_APP_ID" \
  --savepointPath hdfs:///flink/savepoints/heartbeat3-reader \
  "$FLINK_JOB_ID"
```

等待命令成功并记录返回的完整保存点路径，例如 `hdfs:///flink/savepoints/heartbeat3-reader/savepoint-xxxxxx-xxxxxx`。确认旧作业已停止后再启动新作业。如果停止或保存点生成失败，先排查原因，不要用普通启动替代恢复。该操作不是Cancel，也不是 `yarn application -kill`；停止期间Kafka继续接收消息，恢复后仍按保存点位点读取。

### 2. 修改服务器配置

在服务器先备份配置文件，然后打开原文件，将唯一一处 `parallelism=2` 改为 `parallelism=3`：

```bash
cp -p /home/xiong/projects/YX2602_FAULTRECOG/config/local.properties \
  /home/xiong/projects/YX2602_FAULTRECOG/config/local.properties.before-parallelism3

vi /home/xiong/projects/YX2602_FAULTRECOG/config/local.properties

grep -E '^[[:space:]]*(parallelism|kafka.group.id|parser.max.parallelism)[[:space:]]*=' \
  /home/xiong/projects/YX2602_FAULTRECOG/config/local.properties
```

应看到 `parallelism=3`、`kafka.group.id=heartbeat3-reader`。`parser.max.parallelism` 保持原值，原来未配置则继续不配置。只修改这一行，不用本地配置文件整体覆盖服务器的连接参数、凭据及目录。HDFS历史条数策略无需修改，配置同步进程保持运行。

### 3. 从保存点启动3并行作业

将下面引号内路径替换为步骤1实际返回的完整路径，不能只填写父目录：

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/start-heartbeat.sh \
  --from-savepoint 'hdfs:///flink/savepoints/heartbeat3-reader/替换为实际savepoint目录'
```

脚本把 `-s` 放在JAR路径之前传给Flink。仅修改 `local.properties` 不会使运行中的作业改变并行度；不带恢复参数重新提交也不会自动读取该保存点。恢复位点来自Savepoint，不重置Kafka消费位点、不换消费组、不清空Ignite表。

脚本继续使用每个TaskManager 2个槽位。新作业需要至少3个可用槽位，Application模式通常会申请2个TaskManager（共4个槽位），具体以调度器和实际资源配置为准。YARN应有容纳额外TaskManager及Java进程的资源和容器环境；若因资源不足处于等待状态，应检查YARN资源分配，而不是继续重复提交。输入Topic有3个分区，3并行可使每个Source子任务处理1个分区，并将Ignite写入器从2个增加为3个，但吞吐量是否提高仍需运行验证。

### 4. 验收与回退

确认Flink作业为RUNNING，Source和排序/Sink链并行度均为3；Checkpoints页面的Last Restore指向本次保存点，后续Checkpoint继续成功。分别观察3个子任务的 `igniteQueuedRecords`、`igniteCommittedRows`、`igniteSkippedRows` 以及Kafka各分区LAG，比较相同时间窗口的变化。队列持续接近1024或LAG长期增加，仍需定位写入吞吐瓶颈。

保留积压表示继续读取这些消息；现有 `CreatedDate` 超过10分钟过滤规则仍然执行，并不保证积压消息全部入库。Ignite的数据库序号与最近N条历史继续使用库内状态。

如果3并行运行效果更差且需要回退，先对3并行作业执行停止并生成新的Savepoint，改回 `parallelism=2`，从这个最新保存点恢复。不要重新启动旧作业与新作业并行消费。

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

本节是旧持久化方案的现场记录，保留供运维查阅。当前内存窗口不从这些旧文件恢复，不要据此删除共享服务的数据目录。

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

每个sink子任务有一个写入线程、有界FIFO队列和批次缓冲。默认队列最多1024条、活动批次最多128条；最多等待200ms收集一批。同一电梯在批次内和批次间均保持输入顺序，逐条判断新旧和递增序号；只合并事务内对同一槽位的覆盖，保留历史的最终内容不变。队列满时阻塞上游，超过flush等待上限则失败恢复。

检查点将屏障排入同一队列，等待此前全部事务提交后才能成功。小批次在等待时间到达、检查点或正常结束时都会提交。取消作业时不主动刷新剩余缓冲；未完成检查点的数据由Flink恢复重放。

连接、超时等客户端异常会关闭旧客户端并有限重试整个事务，重新连接时再次检查结构。若提交成功但响应丢失，重试时CreatedDate时间比较会跳过已提交记录。重试耗尽或后台线程异常都会传播给Flink，不会把数据库不可用当作坏消息发到异常Topic，也不会静默丢弃。

这是**至少一次交付加数据库事务幂等**，不是Kafka与Ignite之间的分布式两阶段提交。数据库可以领先于最后一个完成的检查点；外部查询也不与Flink检查点对齐。单次SQL查询与业务多表读取的隔离要求应由后续查询模块处理。

| 指标 | 含义 |
| --- | --- |
| igniteCommittedRows | 本次运行中确认新接受的行数 |
| igniteSkippedRows | 数据库判定重复或旧状态的行数 |
| igniteQueuedRecords | 队列当前长度，可能含一个检查点屏障，不含活动批次 |
| igniteCompletedBatches | 成功完成的批次数，包含整批均为重复/旧状态的情况 |
| igniteWriteTimeMs | 成功批次的累计写入耗时，含重试，不含排队/凑批时间 |
| igniteLastBatchSize | 最近成功批次输入的单梯心跳数，包含被跳过的记录 |
| igniteLastBatchDurationMs | 最近成功批次的写入耗时，毫秒 |
| igniteLastCommitAtMs | 最近成功批次返回后的平台Unix毫秒时间，尚无成功批次时为0 |
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
