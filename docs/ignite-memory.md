# Ignite默认内存区域部署

2026-11-09确认：Ignite集群仅供本项目使用。心跳表使用服务端默认数据区域，允许RANDOM_2_LRU在内存压力下淘汰记录，也接受集群全停后窗口丢失；不重建Kafka历史。本文替代旧的heartbeat_memory专用区域部署步骤。

## 当前程序行为

建表SQL不指定DATA_REGION，不硬编码默认区域名称。Ignite使用各节点的defaultDataRegionConfiguration，默认区域可以自定义名称，如Memory_Region。无需再增加heartbeat_memory区域。

正式 `--run` 启动及写入故障后的重新连接都会检查表，自动创建缺失的HB_STATE及CreatedDate索引。只读 `--check-ignite` 不建表。不做定时巡检、不自动删除已有表或修改字段类型；作业重试耗尽后需要重新提交。

Java继续检查ACTIVE、事务模式、同步模式、SQL字段及主键、二进制类型、无TTL以及CreatedDate索引，但不限制区域名称。客户端不能证明区域的持久化或淘汰配置，管理员需核对实际生效的服务端配置。已有结构兼容的表可继续使用原区域；不指定DATA_REGION只决定新建表的区域，不迁移已存在的表。

CreatedDate、UpdatedDate、SendTime均为TIMESTAMP，其他*_MS保持BIGINT；值类型HeartbeatStateRowV4，键类型HeartbeatStateKeyV2。当前仍为PARTITIONED、TRANSACTIONAL、FULL_SYNC、BACKUPS=0。

## 服务端配置

截图中的默认区域配置为下列内容，应位于DataStorageConfiguration内部。已有该配置时无需重复添加。各服务端节点需核对实际加载的配置及容量，应用不会修改XML或启动Ignite。

```xml
<property name="defaultDataRegionConfiguration">
    <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
        <property name="name" value="Memory_Region"/>
        <property name="initialSize" value="#{2L * 1024 * 1024 * 1024}"/>
        <property name="maxSize" value="#{32L * 1024 * 1024 * 1024}"/>
        <property name="persistenceEnabled" value="false"/>
        <property name="pageEvictionMode" value="RANDOM_2_LRU"/>
        <property name="evictionThreshold" value="0.7"/>
    </bean>
</property>
```

2GiB初始、32GiB上限均为每个节点的值，不是整个集群共用的32GiB。区域使用堆外内存，另需容纳JVM堆、其他区域和系统内存。0.7是淘汰阈值，不代表一次删除70%的记录，也不是严格的进程内存限制。允许淘汰可降低区域耗尽概率，但不能保证不会OOM或发生事务冲突。调整XML需按集群维护流程重启加载。

## 已确认的集群目录

2026-11-09现场确认AI-slave06上的容器名为ignite-node-1：

| 项目 | 宿主机路径 | 容器路径 |
| --- | --- | --- |
| 配置文件bind挂载 | /home/workdir/ignite2/cluster/config/default-config.xml | /opt/ignite/apache-ignite/config/default-config.xml |
| 工作目录bind挂载 | /home/workdir/ignite2/cluster/ignite-data | /opt/ignite/work |

IGNITE_HOME为 `/opt/ignite/apache-ignite`，启动命令为 `/bin/sh -c "$IGNITE_HOME/run.sh"`。已观察的环境变量未显示CONFIG_URI；挂载文件存在不能替代核对启动脚本实际加载的文件和运行指标。其他节点路径和容器名需要分别确认，不假设与node-1相同。

```bash
docker exec ignite-node-1 cat /opt/ignite/apache-ignite/run.sh
docker inspect ignite-node-1 --format 'Mounts={{json .Mounts}} Cmd={{json .Config.Cmd}}'
```

修改宿主机挂载源并由管理员加载后，核对容器文件及本次启动日志：

```bash
docker exec ignite-node-1 cat /opt/ignite/apache-ignite/config/default-config.xml
ignite_started_at=$(docker inspect --format '{{.State.StartedAt}}' ignite-node-1)
docker logs --since "$ignite_started_at" ignite-node-1 2>&1 | grep -A 2 'Memory_Region'
```

预期persistence=false，32GiB配置对应maxCfg=32768MB。日志可能要等待下一轮周期指标才出现。

## 更新和启动

1. 停止所有旧心跳作业。
2. 更新 `target/heartbeat-reader-0.1.0.jar`、sha256和 `sql/ignite2-heartbeat.sql`，保留服务器自己的local.properties及性能参数。按实际对外地址配置ignite.addresses，可填写多个以逗号分隔的节点客户端地址。
3. 表不存在时直接启动，由程序创建。已有表若使用heartbeat_memory或旧字段类型，想切换到默认区域需由管理员执行下面SQL清空该表；不要删除整个Ignite工作目录。

   ```sql
   DROP TABLE IF EXISTS PUBLIC.HB_STATE;
   ```

4. 本次按新窗口上线。若要跳过Kafka积压，使用从未用过的新kafka.group.id且不加载Savepoint/Checkpoint；当前代码将缺失消费位点初始化到启动时末尾。沿用旧组会继续读取已提交位点后的消息，不会因切换区域而自动跳过积压。

   ```bash
   bash /home/xiong/projects/YX2602_FAULTRECOG/bin/start-heartbeat.sh
   ```

无需为程序取消区域指定而重启已正确配置的Ignite。服务端配置要变更时才由管理员安排节点重启。运行账号需有建表、建索引、查询和写入权限。

## 淘汰后的数据语义

RANDOM_2_LRU按数据页选择淘汰，不区分最新与历史槽位，不能保证只淘汰最旧历史。每梯最近N条是写入目标，不保证时刻齐全；SLOT=-1也可能消失，不再上报的电梯可能从表中消失。

最新槽位丢失后，下次有效心跳从空状态开始，DB_SEQUENCE重新从1递增，并采用当时加载的历史条数策略。未丢失的其他历史槽位不会为此全部清空；重建期间可能混合新旧序号，DB_SEQUENCE不可用作跨丢失事件的稳定唯一标识或时间排序。历史查询按CreatedDate排序，过滤SLOT>=0；查最新快照使用SLOT=-1。新心跳逐渐覆盖历史，但不承诺恢复缺失记录。

历史条数调整仍在电梯下一条被接受的新心跳事务内执行；最新槽位和其策略元数据若已丢失，就无法依赖它识别旧窗口大小，动态缩容不再具有完整窗口模式下的严格清理保证。容量管理以服务端内存区域和已接受的淘汰策略为准。

Flink的CreatedDate去重状态独立于Ignite。Ignite淘汰或重启不会清空Flink状态，只有通过时间、校验及顺序判断的后续心跳才入库。已排队或恢复重放且被允许写入的数据也可能建立窗口，不承诺恢复后表中只有重启后产生的消息。默认超过10分钟的源数据仍按既有规则过滤；该过滤不定时删除已入库旧行。

BACKUPS=0在多节点集群中表示没有额外备份副本，单节点丢失也可能丢分区数据，且可能触发分区丢失保护影响后续访问。自动建缺失整表不能修复仍存在但已标记LOST的分区；出现该状态需管理员按集群策略处理。不在本次区域修改中更改备份数、分区丢失策略或自动重置分区。

## 验收

```sql
SELECT CACHE_NAME, DATA_REGION_NAME FROM SYS.CACHES WHERE CACHE_NAME='HB_STATE';
SELECT EID, COUNT(*) FROM PUBLIC.HB_STATE GROUP BY EID ORDER BY COUNT(*) DESC LIMIT 20;
SELECT EID, CREATEDDATE, SENDTIME, DB_SEQUENCE FROM PUBLIC.HB_STATE WHERE SLOT=-1 LIMIT 20;
```

省略DATA_REGION后，SYS.CACHES中的DATA_REGION_NAME可显示NULL，表示未显式指定区域，并非缺少区域；实际使用服务端配置的默认区域（截图为Memory_Region），结合节点运行配置及指标核实。无淘汰、未丢失元数据且配置N=10时每梯最多11行；允许淘汰后行数不足不代表解析失败。CreatedDate索引、动态策略、事务写入及监控继续保留。集群吞吐和真实压力下淘汰行为需在部署后观测。

## 历史目录记录

旧单节点容器名为ignite-single，历史容器ID为675e6559ebcf。旧数据挂载 `/home/workdir/ignite2/standalone/ignite-data -> /opt/ignite/work`，旧配置位于容器 `/opt/ignite/apache-ignite/config/default-config.xml`，当时未单独挂载配置文件。旧持久化数据目录 `/home/workdir/ignite2/standalone/ignite-data/db` 位于/dev/sdb1、XFS的/home挂载。这些是旧部署记录，不作为当前集群配置修改目标。

旧heartbeat和heartbeat_memory区域不再是当前新版建表的必需配置。默认内存区域不恢复旧业务窗口，但Ignite日志、元数据、其他内部文件及Flink的HDFS检查点仍可能写盘。

## 本地验证和部署包

2026-11-09使用Java 11执行 `mvn -o -B -ntp clean verify`，196项测试通过，无失败、错误或跳过。真实Ignite 2.18.0使用自定义名称Memory_Region作为默认区域，开启RANDOM_2_LRU和0.7阈值，不配置heartbeat_memory；验证自动建表的实际运行区域、TIMESTAMP字段及索引、原客户端在服务重启后自动重建表、最终JAR独立进程建表写入。另用定向删除记录模拟部分窗口丢失，验证后续心跳仍可写入和重新积累；该模拟不代表实际压力淘汰顺序或集群节点故障测试。

部署包为 `dist/heartbeat-reader-0.1.0-java11-default-region-deploy.zip`，包含JAR、sha256、SQL、脚本及更新文档，不包含服务器实际local.properties。尚未修改集群，真实压力下的淘汰、集群拓扑与分区丢失行为需要集群验收。
