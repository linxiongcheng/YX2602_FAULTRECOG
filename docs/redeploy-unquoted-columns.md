# 无引号字段：清空重建与重新部署

本页保留字段结构升级操作。当前版本还要求服务端新增 `heartbeat_memory` 内存区域；首次切换与全停后重建须先按 [内存模式部署说明](ignite-memory.md) 执行，不能只替换JAR或SQL。

适用于本次不保留旧数据、不恢复旧Flink状态的部署。表仍为 `PUBLIC.HB_STATE`，字段采用不带双引号的SQL标识符。Ignite保存为大写，查询可以直接使用 `eid`、`EId` 或 `EID`。原始Kafka JSON字段名和解析规则不变。

建表脚本、Java缓存字段映射和启动结构校验已经同步更新。当前二进制类型为 `HeartbeatStateKeyV2` / `HeartbeatStateRowV4`，值类型V4的CreatedDate、UpdatedDate、SendTime均使用TIMESTAMP，避免复用旧String/Long类型元数据。必须同时更新JAR和SQL，旧JAR不能写新表。

## 1. 停止旧YARN作业

在提交节点找到当前心跳作业的Application ID，只停止该作业：

```bash
yarn application -list
yarn application -kill application_替换为旧心跳作业实际ID
yarn application -status application_替换为旧心跳作业实际ID
```

确认旧应用已经终止（例如KILLED、FINISHED或FAILED），没有其他写入该表的心跳作业后，再执行下面的删表操作。本次不生成或加载旧Savepoint。无需重启Ignite服务，也无需删除其磁盘目录。

## 2. 更新部署文件

将本次构建的文件复制到提交节点的固定位置：

```text
/home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar
/home/xiong/projects/YX2602_FAULTRECOG/target/heartbeat-reader-0.1.0.jar.sha256
/home/xiong/projects/YX2602_FAULTRECOG/sql/ignite2-heartbeat.sql
/home/xiong/projects/YX2602_FAULTRECOG/bin/watch-history-config.sh
```

保留服务器现有 `config/local.properties` 中的实际连接配置。部署包不包含该私有配置文件。可在提交节点校验JAR：

```bash
cd /home/xiong/projects/YX2602_FAULTRECOG/target
sha256sum -c heartbeat-reader-0.1.0.jar.sha256
```

如选择在服务器从更新后的完整源码构建，先设置Java 11，再运行 `mvn -B -ntp clean verify`；使用本次已经构建好的部署包则无需再次编译。

## 3. 在现有Ignite服务中删表并重建

通过已有SQL客户端连接与 `ignite.addresses` 对应的集群，以有建表权限的账号执行：

```sql
DROP TABLE IF EXISTS PUBLIC.HB_STATE;
```

此操作删除该表及其中全部最新状态和历史数据。不会删除其他业务表。若有其他业务使用HB_STATE，应先停止相关读写。

然后在SQLLine中执行新版建表脚本：

```text
!run /home/xiong/projects/YX2602_FAULTRECOG/sql/ignite2-heartbeat.sql
```

路径是SQLLine客户端所在机器可读取的路径。如果使用DBeaver等SQL工具，打开新版 `sql/ignite2-heartbeat.sql` 并执行其中的完整CREATE TABLE语句。

不要把DROP语句加入作业启动流程。建表脚本末尾 `WITH "..."` 的双引号属于Ignite表选项语法，应保留；所有字段定义和主键字段都不带双引号。

建表后先验证以下查询能够执行，尚未启动新作业时没有数据是正常的：

```sql
SELECT eid, slot, createddate, floor, mainfaultcode, part_day
FROM public.hb_state
WHERE eid = 54317;
```

表依然使用现有 `heartbeat` 数据区域，持久化设置和磁盘目录继续由Ignite服务端决定。

## 4. 配置和预检

编辑 `/home/xiong/projects/YX2602_FAULTRECOG/config/local.properties`。如果希望从本次启动时的Kafka末尾开始，使用一个从未使用过的新消费组，例如：

```properties
kafka.group.id=yx2602-heartbeat-unquoted-20261104
```

该名称只是示例，确保集群中未使用过。沿用旧消费组会从其已提交位点继续；无论哪种方式，超过10分钟的数据仍被过滤。本次不携带 `-s` 参数恢复旧状态。

若启用了HDFS历史条数动态配置，按 [历史条数热更新](heartbeat-ignite.md#历史条数热更新) 确保共享文件已发布且同步进程正在运行；现有正常同步进程无需重复启动。首次部署时执行一次初始化，再启动一个持续同步进程。

在提交节点执行数据库预检：

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

看到“Ignite连接与缓存结构校验通过”后再提交。若出现类型或字段结构不匹配，检查是否执行了新版建表脚本，且SQL客户端与Flink连接的是同一个Ignite集群。

## 5. 重新提交YARN Application

所有可能运行YARN容器的节点均需存在 `/opt/java11`。在完成上述环境设置的同一终端执行：

```bash
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

检查Web UI中作业为RUNNING、Checkpoint持续成功，并等待设备上报新的有效心跳。清空的数据不会自动回填。Checkpoint使用现有配置目录下的新JobID子目录，不需要删除其他作业或旧作业的检查点。

## 6. 不加引号查询

```sql
-- 最新状态
SELECT eid, createddate, floor, mainfaultcode, part_day
FROM public.hb_state
WHERE eid = 54317 AND slot = -1;

-- 最近N条历史，包含最新状态
SELECT eid, slot, createddate, floor, mainfaultcode, db_sequence
FROM public.hb_state
WHERE eid = 54317 AND slot >= 0
ORDER BY created_time_ms DESC;
```

将54317替换为实际电梯ID。CreatedDate保存原始日期文本，时间排序使用数值列 `created_time_ms`，避免按非补零日期字符串排序。最新与历史继续由slot区分，每台电梯最多N+1行，历史条数热更新规则不变。
