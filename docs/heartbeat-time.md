# 心跳业务时间：CreatedDate

项目以 `CreatedDate` 为唯一业务时间，按北京时间 `Asia/Shanghai` 解析。未来偏差检查、10分钟过期过滤、Flink每梯新旧判断、Ignite防旧状态覆盖，全部使用同一个 `createdTimeMs`。

```json
{"CreatedDate":"2026/1/22 17:56:17","SendTime":"","EId1":773282}
```

该日期对应UTC `2026-01-22T09:56:17Z`。解析不依赖服务器默认时区。日期格式为 `yyyy/M/d H:mm:ss`，月、日、小时允许一位或两位，分钟和秒固定两位；可带1～3位小数秒，如 `.1`、`.12`、`.123`。两侧空白会在解析时去除，原始日期文本仍保留。严格拒绝不存在的日期、24点、闰秒、超过毫秒精度的小数，以及未约定的其他格式。

| 输入情况 | 行为 |
| --- | --- |
| CreatedDate缺失、null、空白 | MISSING_CREATED_DATE，写异常Topic |
| CreatedDate类型/日期/格式非法 | INVALID_CREATED_DATE，写异常Topic |
| CreatedDate超前平台时钟超过60秒（默认） | FUTURE_CREATED_DATE，写异常Topic |
| CreatedDate距处理时间超过10分钟（默认） | 过滤并计数，不写异常Topic |
| 同一电梯CreatedDate相同或更早 | 过滤并计数，不增加状态序号 |
| SendTime缺失、空字符串、null或值非法 | 不影响业务时间校验和入库 |

无论SendTime、UpdatedDate或Kafka时间是否可用，都不替代非法CreatedDate。合法历史日期先通过读取层，在解析层按年龄过滤。10分钟和60秒的边界均包含等号。

`SendTime`仅保留为追踪信息：正64位整数保存在可空的 `sendTimeMs`，其他值为null；`sendTimeRaw` 保存字段值的JSON表示，不承诺原始空格或数值词法形式。字段缺失时为null，空字符串保存为JSON字符串的表示，显式JSON null保存为字符串 `null`。完整原始消息仍由读取层保留。

同一电梯若只提供秒精度的CreatedDate，一秒内最多接受一条状态；后续同秒消息不会用SendTime或offset打破平局。这沿用“时间必须严格更新”的规则。

## 状态与存储

按新项目设计，不实现旧SendTime版本状态迁移。Flink状态类为 `CreatedVersion`，状态名为 `last-accepted-created-version-v1`，每梯序号从1开始。后续本项目的检查点/保存点可以恢复CreatedDate基准和序号。

数据库使用 [建表脚本](../sql/ignite2-heartbeat.sql) 中的单表 `PUBLIC.HB_STATE`，每梯SLOT=-1保存最新状态，SLOT>=0保存默认最多10条历史；保留条数支持热更新，调小后在该梯下一条新心跳被接受时事务内清理，详见 [存储模块说明](heartbeat-ignite.md#历史条数热更新)。`CREATED_TIME_MS` 是比较依据，`createddate` 保留日期文本，`part_day` 是按北京时间生成的yyyy-MM-dd日期。`sendtime` 和 `SEND_TIME_RAW` 仅用于追踪。数据库独立维护DB_SEQUENCE，不使用Flink序号决定历史槽位。

首次部署按 [内存模式部署说明](ignite-memory.md) 准备服务端区域并提交，不携带旧项目保存点。正式作业自动创建缺失空表，也可手工执行建表脚本；应用不修改既有表结构，同名表结构或区域不匹配时明确报错。

```sql
SELECT eid, createddate, CREATED_TIME_MS, part_day,
       sendtime, SEND_TIME_RAW, DB_SEQUENCE, FLINK_SEQUENCE
FROM PUBLIC.HB_STATE WHERE SLOT = -1 ORDER BY eid LIMIT 20;
```

日期解析、空SendTime、两种时间顺序冲突、10分钟边界、未来60秒边界、CreatedDate状态恢复，以及真实Ignite的新旧判断均有自动化测试。内网Kafka/YARN和吞吐量需在集群验收。

## 验证结果

2026-11-04在Java 11下执行 `mvn clean verify`：152项测试全部通过，失败、错误和跳过均为0。覆盖CreatedDate规则、Flink状态恢复、真实Ignite单表事务/持久化、动态配置后台刷新、历史扩缩容、离线延迟清理、策略版本保护及事务回滚。新增三个端口从解析到SQL存储的41个文档字段验证，以及最新/历史槽位隔离、N+1行数和北京时间跨日的part_day验证。同步程序验证只修改local.properties即可发布、版本自动管理、无效配置保护及凭据隔离；最终JAR经独立Java进程验证配置发布、数据库写入与SQL查询，应用class版本为55。热加载测试使用本地临时文件；HDFS发布和读取、内网Kafka/YARN及吞吐量仍需集群验收。
