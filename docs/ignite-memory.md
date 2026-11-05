# Ignite 心跳内存模式

2026-11-05确认：心跳只保存最新状态和最近N条有效更新，不要求Ignite全停后的数据恢复，不从Kafka历史重建窗口。全停后电梯随新心跳重新出现；离线电梯保持无数据。默认N=10，每梯最多11行（最新状态1行、历史最多10行）。

当前CreatedDate、UpdatedDate为TIMESTAMP，值类型为HeartbeatStateRowV3，并自动创建 `IDX_HB_STATE_CREATEDDATE`。旧VARCHAR表即使已经位于内存区域也需要按下面流程重建。时区、非法日期处理和查询示例见 [时间类型与索引](heartbeat-time.md#createddate索引与时间范围查询)。

## 已核对的容器配置位置

2026-11-05根据 `AI-slave06` 上的现场命令截图确认：

| 项目 | 已核对的信息 |
| --- | --- |
| 容器ID | `675e6559ebcf`，容器重建后需重新确认 |
| IGNITE_HOME | `/opt/ignite/apache-ignite` |
| 容器内默认配置文件 | `/opt/ignite/apache-ignite/config/default-config.xml`，已确认存在 |
| Docker WorkingDir | `/opt/ignite`，不能用它直接推导XML位置 |
| Docker启动命令 | `/bin/sh -c "$IGNITE_HOME/run.sh"` |
| 数据目录挂载 | `/home/workdir/ignite2/standalone/ignite-data -> /opt/ignite/work` |
| 配置文件挂载 | 当前Mounts输出未显示配置文件或配置目录挂载 |
| CONFIG_URI | 本次环境变量查询未显示该变量 |

`/opt/ignite/config/default-config.xml` 已确认不存在。宿主机历史候选路径 `/home/lwh/apache_ignite/ignite-2.18.0/config/default-config.xml` 未证实与该容器关联，不应直接当作当前配置修改目标。

在宿主机执行下面的一行命令查看默认XML：

```bash
docker exec 675e6559ebcf cat /opt/ignite/apache-ignite/config/default-config.xml
```

需要确认启动脚本如何选择配置时执行：

```bash
docker exec 675e6559ebcf cat /opt/ignite/apache-ignite/run.sh
```

同日后续XML截图已确认文件中的配置：`workDirectory=/opt/ignite/work`，`walMode=FSYNC`；`dataRegionConfigurations` 列表中只有旧 `heartbeat` 区域，`persistenceEnabled=true`，`initialSize=67108864`（64MiB），`maxSize=268435456`（256MiB）。该文件尚未添加 `heartbeat_memory`，尚未完成内存模式切换。这些是文件内容核对结果，启动脚本选择及运行节点是否加载了最新文件仍需由管理员确认。

应在现有列表中、旧 `heartbeat` 的 `</bean>` 之后与 `</list>` 之前追加下节的 `heartbeat_memory` 配置，保留旧区域与全局 `walMode=FSYNC`。新内存区域不因全局保留FSYNC就启用数据持久化。

该XML当前没有单独挂载，修改前由管理员备份；同一容器重启通常保留容器文件，删除重建容器则不能依赖这些修改，长期配置应纳入宿主机挂载或镜像部署管理。

## 服务管理员需要配置的内容

本项目只连接已有Ignite 2.18.0服务，不启动或重启服务。请管理员在**实际加载的服务端配置**中，为所有承载心跳缓存的节点增加名为 `heartbeat_memory` 的DataRegion。下面是应合并到现有 `DataStorageConfiguration.dataRegionConfigurations` 列表中的配置片段，不是整份启动配置；不要覆盖其他区域或其他业务的持久化设置。

```xml
<bean class="org.apache.ignite.configuration.DataRegionConfiguration">
    <property name="name" value="heartbeat_memory"/>
    <property name="persistenceEnabled" value="false"/>
    <property name="initialSize" value="268435456"/>
    <property name="maxSize" value="2147483648"/>
    <property name="pageEvictionMode" value="DISABLED"/>
</bean>
```

256MiB初始内存、2GiB上限只是示例，管理员应按电梯数量、N、字段和索引开销及服务器/容器可用内存确定容量。它使用堆外内存，不能只看Java堆大小；已离线电梯不会自动删除，因此应按累计保留的电梯数规划容量。内存不足会导致写入失败，不能依靠任意淘汰缓存条目维持运行，否则会破坏最新状态和历史窗口的一致性。

新增区域需要管理员按现有集群流程重启相关节点。共享服务可能因为其他持久化区域而仍需激活为ACTIVE，由管理员完成。客户端无法通过缓存配置核实区域的 `persistenceEnabled`，需管理员核对生效配置。区域名称正确不能替代这个确认。

只有HB_STATE的业务页使用该内存区域。服务日志、元数据、其他缓存以及Flink的HDFS检查点仍可能写磁盘，不能要求整机磁盘写入归零。不要用关闭整个服务WAL代替内存区域配置。

## 本次从旧持久化表切换

1. 停止当前心跳YARN应用，确认没有旧心跳作业继续写HB_STATE。本次不保存或加载Savepoint，不恢复旧窗口。
2. 管理员配置并启用 `heartbeat_memory` 区域，确认 `persistenceEnabled=false`。
3. 旧表的数据区域不能通过 `CREATE TABLE IF NOT EXISTS` 改变。用户已接受清空本项目心跳数据，由管理员只删除本项目的旧表：

   ```sql
   DROP TABLE IF EXISTS PUBLIC.HB_STATE;
   ```

   不删除Ignite整个work/db/WAL目录，不影响其他表。程序不会自动DROP或迁移已有表。
4. 更新项目 `target/heartbeat-reader-0.1.0.jar`、对应sha256及 `sql/ignite2-heartbeat.sql`。保留服务器自己的 `config/local.properties` 和性能参数，不用本地文件整份覆盖。
5. 将 `kafka.group.id` 改为一个从未使用过的名称，例如 `heartbeat3-memory-20261105-01`。如果示例名已经使用过，另取新名称。代码会将缺失消费位点初始化到本次启动时各分区的末尾。
6. 确保历史配置发布程序和共享HDFS文件可用，然后直接提交：

   ```bash
   bash /home/xiong/projects/YX2602_FAULTRECOG/bin/start-heartbeat.sh
   ```

   不加 `--from-savepoint` / `-s`。无需删除旧Kafka组或HDFS检查点目录。

正式 `--run` 会在连接后自动创建不存在的空HB_STATE，并检查完整结构。它使用与手工SQL相同的脚本，脚本已打进JAR，多Sink同时启动可安全执行。运行账号需要该表的创建、查询和读写权限。若服务不允许应用建表，管理员应在每次表消失后先执行建表脚本，再启动作业。

已有HB_STATE如果仍在旧 `heartbeat` 区域，启动会报结构不匹配，不会静默沿用旧持久化表。`--check-config` 只校验本地配置；`--check-ignite` 保持只读，不建表，表不存在时会报错。需要启动前使用 `--check-ignite` 时，先由管理员执行建表脚本。

## 内存集群全停后的日常操作

计划重启：先停止心跳作业，由管理员重启服务，再用新的消费组直接启动心跳作业，不携带恢复参数。表丢失时程序自动建空表，不进行任何历史数据重建。

第一条被接受的新心跳会建立该电梯最新状态和第一条历史，之后逐步补齐到N条；不足N条时不填充。CreatedDate、10分钟过期过滤和同梯严格递增判断继续生效。DB_SEQUENCE在该电梯空状态上从1开始。

现有Flink检查点、异常消息交付和短暂连接故障重试仍正常工作，但不承诺恢复Ignite丢失的窗口。意外重启时，若Flink尚在重试，客户端重新建立连接后可创建缺失表并继续处理；若任务已失败，人工重新提交。若要严格跳过停机期间所有消息，应停止旧任务，用新组且不携带恢复参数重提。单纯删除Ignite数据不会自动重置Kafka位置。

当前 `BACKUPS=0`，多节点内存集群中单节点丢失也可能丢失该节点上的分区，并非只有全停才丢数据。此版本按当前单节点服务使用；多节点部署需管理员另行确定副本与分区丢失策略。

## 验收

管理员确认有效区域配置为 `persistenceEnabled=false`；SQL查看表的数据区域：

```sql
SELECT CACHE_NAME, DATA_REGION_NAME FROM SYS.CACHES WHERE CACHE_NAME='HB_STATE';
SELECT EID, COUNT(*) FROM PUBLIC.HB_STATE GROUP BY EID ORDER BY COUNT(*) DESC LIMIT 20;
SELECT EID, CREATEDDATE, DB_SEQUENCE FROM PUBLIC.HB_STATE WHERE SLOT=-1 LIMIT 20;
```

区域应为 `heartbeat_memory`。N=10且未动态缩容时，每梯最多11行。第一次有效心跳为2行（最新和历史各1）；不应等待10条到齐才更新最新状态。

性能验收保持并行度、批大小、输入量及N一致，对比 `committed_per_s`、`commit_avg_ms`、队列、读取延迟与过期比例。纯内存去除了该区域业务数据的持久化开销，但仍有事务、索引、网络和同步读取成本，不保证一定达到所需吞吐。

之前核实的 `/home/workdir/ignite2/standalone/ignite-data/db` 是旧持久化方案的目录记录。新内存窗口不以该目录中的旧分区文件恢复；旧目录由服务管理员管理，不作为本次上线的清理对象。

## 本地验证与产物

2026-11-05 TIMESTAMP及CreatedDate索引版通过Java 11全量 `mvn -o -B -ntp clean verify`：191项测试全部通过。新增真实SQL时间类型/毫秒/空值读写、跨客户端默认时区、时间范围与EXPLAIN索引命中、缺失索引补建、错误同名索引拒绝、旧VARCHAR表重建与二进制元数据隔离，以及UpdatedDate非法值分流验证。

2026-11-05以Java 11执行 `mvn -o -B -ntp clean verify`，178项测试通过，失败、错误、跳过均为0。真实Ignite 2.18.0内存节点覆盖事务写入、窗口条数、全停丢失及新心跳重建、已有客户端重连后建空表、多个写入器并发建表、只读检查不创建表、错误区域拒绝，以及最终JAR独立进程使用内嵌SQL建表并读写。保留批量事务回滚、同梯并发和动态条数的测试。

部署包为 `dist/heartbeat-reader-0.1.0-java11-memory-deploy.zip`，含JAR、校验值、SQL、脚本、示例配置和本文档，不包含实际 `local.properties`。服务端真实配置、内网YARN/Kafka/HDFS连接及吞吐尚需集群验收；本地测试不会修改用户集群。
