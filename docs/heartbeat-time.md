# 心跳业务时间：CreatedDate

项目以 `CreatedDate` 为唯一业务时间，按北京时间 `Asia/Shanghai` 解析。未来偏差检查、10分钟过期过滤、Flink每梯新旧判断、Ignite防旧状态覆盖，全部使用同一个 `createdTimeMs`。

```json
{"CreatedDate":"2026/1/22 17:56:17","SendTime":"","EId1":773282}
```

该日期对应UTC `2026-01-22T09:56:17Z`。解析不依赖服务器默认时区。日期格式为 `yyyy/M/d H:mm:ss`，月、日、小时允许一位或两位，分钟和秒固定两位；可带1～3位小数秒，如 `.1`、`.12`、`.123`。两侧空白会在解析时去除；原始日期文本保留在消息和预览模型中，SQL列保存TIMESTAMP。严格拒绝不存在的日期、24点、闰秒、超过毫秒精度的小数，以及未约定的其他格式。

| 输入情况 | 行为 |
| --- | --- |
| CreatedDate缺失、null、空白 | MISSING_CREATED_DATE，写异常Topic |
| CreatedDate类型/日期/格式非法 | INVALID_CREATED_DATE，写异常Topic |
| CreatedDate超前平台时钟超过60秒（默认） | FUTURE_CREATED_DATE，写异常Topic |
| CreatedDate距处理时间超过10分钟（默认） | 过滤并计数，不写异常Topic |
| 同一电梯CreatedDate相同或更早 | 过滤并计数，不增加状态序号 |
| SendTime缺失、空字符串、null或值非法 | 不影响业务时间校验和入库 |
| UpdatedDate缺失、null、空白 | SQL保存NULL |
| UpdatedDate非空但类型/格式/日期非法 | 解析阶段INVALID_FIELD，error_field=UpdatedDate，写异常Topic |

无论SendTime、UpdatedDate或Kafka时间是否可用，都不替代非法CreatedDate。合法历史日期先通过读取层，在解析层按年龄过滤。10分钟和60秒的边界均包含等号。

`SendTime`仅保留为追踪信息：正64位整数保存在可空的 `sendTimeMs`，其他值为null；`sendTimeRaw` 保存字段值的JSON表示，不承诺原始空格或数值词法形式。字段缺失时为null，空字符串保存为JSON字符串的表示，显式JSON null保存为字符串 `null`。完整原始消息仍由读取层保留。

同一电梯若只提供秒精度的CreatedDate，一秒内最多接受一条状态；后续同秒消息不会用SendTime或offset打破平局。这沿用“时间必须严格更新”的规则。

## 状态与存储

按新项目设计，不实现旧SendTime版本状态迁移。Flink状态类为 `CreatedVersion`，状态名为 `last-accepted-created-version-v1`，每梯序号从1开始。后续本项目的检查点/保存点可以恢复CreatedDate基准和序号。

数据库使用 [建表脚本](../sql/ignite2-heartbeat.sql) 中的单表 `PUBLIC.HB_STATE`，每梯SLOT=-1保存最新状态，SLOT>=0保存默认最多10条历史；保留条数支持热更新，调小后在该梯下一条新心跳被接受时事务内清理，详见 [存储模块说明](heartbeat-ignite.md#历史条数热更新)。`CREATED_TIME_MS` 是比较依据；`CREATEDDATE`、`UPDATEDDATE` 为TIMESTAMP，Java写入类型为java.sql.Timestamp，保留毫秒精度。`part_day` 是按北京时间生成的yyyy-MM-dd日期。`sendtime`、`*_MS`仍为BIGINT；`SEND_TIME_RAW`仅用于追踪。数据库独立维护DB_SEQUENCE，不使用Flink序号决定历史槽位。

UpdatedDate采用与CreatedDate相同的北京时间格式，但不参与排序、过期或未来时间判断，也不要求晚于CreatedDate。空白归一为NULL；非空非法值在解析阶段进入异常Topic，避免进入写入队列后使整个批次失败。

首次部署按 [内存模式部署说明](ignite-memory.md) 准备服务端区域并提交，不携带旧项目保存点。正式作业自动创建缺失空表，也可手工执行建表脚本；应用不修改既有表结构，同名表结构或区域不匹配时明确报错。

```sql
SELECT eid, createddate, CREATED_TIME_MS, part_day,
       sendtime, SEND_TIME_RAW, DB_SEQUENCE, FLINK_SEQUENCE
FROM PUBLIC.HB_STATE WHERE SLOT = -1 ORDER BY eid LIMIT 20;
```

日期解析、空SendTime、两种时间顺序冲突、10分钟边界、未来60秒边界、CreatedDate状态恢复，以及真实Ignite的新旧判断均有自动化测试。内网Kafka/YARN和吞吐量需在集群验收。

## CreatedDate索引与时间范围查询

建表脚本包含单列有序索引，正式启动或内存服务重启后会自动补建缺失索引；`--check-ignite`只检查，不补建。启动通过 `SYS.INDEXES` 核实名称和首列，已有同名错误索引会报错，不会自动删除。

```sql
CREATE INDEX IF NOT EXISTS IDX_HB_STATE_CREATEDDATE
ON PUBLIC.HB_STATE (CREATEDDATE);

SELECT EID, CREATEDDATE, UPDATEDDATE
FROM PUBLIC.HB_STATE
WHERE SLOT = -1
  AND CREATEDDATE >= TIMESTAMP '2026-11-05 16:00:00'
  AND CREATEDDATE <  TIMESTAMP '2026-11-05 16:10:00'
ORDER BY CREATEDDATE;

EXPLAIN SELECT EID FROM PUBLIC.HB_STATE
WHERE CREATEDDATE >= TIMESTAMP '2026-11-05 16:00:00'
  AND CREATEDDATE <  TIMESTAMP '2026-11-05 16:10:00';
```

以上查询边界以SQL运行环境为北京时间为前提。TIMESTAMP不包含时区标识；程序用明确的Asia/Shanghai规则解析源文本，再用绝对毫秒构造Timestamp，客户端默认时区不会改变写入时刻。Ignite服务端与SQL/JDBC客户端应统一使用 `Asia/Shanghai`（例如JVM参数 `-Duser.timezone=Asia/Shanghai`），保证SQL时间字面量及显示一致。跨时区Java客户端可传入Timestamp参数并明确按北京时间格式化结果，不要手工加减8小时。

查询历史将 `SLOT=-1` 改为 `SLOT>=0`，避免最新状态与历史重复计数。时间范围条件直接作用于CREATEDDATE，避免先对该列格式化或CAST为字符串。实际查询是否选择索引以EXPLAIN为准；小表或低选择性范围也可能选择其他计划。索引覆盖最新与历史两种槽位，会增加内存和每次写入的维护开销，不是写入加速措施。

## 从旧VARCHAR表切换

本次按无旧数据部署：停止旧心跳作业，由管理员DROP旧HB_STATE，替换JAR及SQL，再按 [内存模式](ignite-memory.md) 使用新消费组且不带Savepoint启动，自动建TIMESTAMP表及索引。不要仅执行CREATE TABLE IF NOT EXISTS，它不会把已有VARCHAR列改成TIMESTAMP。无需更改local.properties中的字段类型配置。

值类型升级为 `HeartbeatStateRowV3`，避免Ignite保留的旧二进制元数据仍把两个日期字段识别为String；键类型继续为 `HeartbeatStateKeyV2`。旧表会被启动校验拒绝，不迁移旧值，不清理共享集群的全局元数据。

## 验证结果

2026-11-05 TIMESTAMP及索引版本在Java 11下执行 `mvn -o -B -ntp clean verify`，191项测试通过，失败/错误/跳过均为0。真实Ignite验证Timestamp类型、毫秒精度、UpdatedDate空值和非法值分流、跨JVM默认时区读写、范围边界及EXPLAIN使用索引、缺失索引补建、旧String二进制元数据与新值类型隔离；最终JAR使用UTC客户端独立进程验收。

2026-11-04在Java 11下执行 `mvn clean verify`：152项测试全部通过，失败、错误和跳过均为0。覆盖CreatedDate规则、Flink状态恢复、真实Ignite单表事务/持久化、动态配置后台刷新、历史扩缩容、离线延迟清理、策略版本保护及事务回滚。新增三个端口从解析到SQL存储的41个文档字段验证，以及最新/历史槽位隔离、N+1行数和北京时间跨日的part_day验证。同步程序验证只修改local.properties即可发布、版本自动管理、无效配置保护及凭据隔离；最终JAR经独立Java进程验证配置发布、数据库写入与SQL查询，应用class版本为55。热加载测试使用本地临时文件；HDFS发布和读取、内网Kafka/YARN及吞吐量仍需集群验收。
