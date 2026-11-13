# 消费、解析、Ignite写入统计

统计命令从现有采集器的本地监控日志读取数据，不连接Kafka、Ignite或Flink，不修改消费位点、表和运行参数。可一次生成Markdown报告，也可每30秒覆盖刷新同一报告文件，报告不会持续追加增长。消费/解析/写入埋点已经存在，无需为本功能重新提交Flink作业。

## 内网准备

同步新增源码 `src/main/java/com/hitachi/yx2602/heartbeat/HeartbeatPerformanceReport.java` 和脚本 `bin/report-heartbeat-performance.sh`，用Java 11在内网构建项目。若日志采集器正在使用本目录的JAR，先停止并记下其应用ID和REST地址，编译完成后重启采集器；Flink运行的是已上传到YARN的JAR副本，无需停止业务作业。

```bash
source /opt/flink-1.20.5/conf/cdh-env.sh
export JAVA_HOME=/opt/java11
export PATH=/opt/java11/bin:$PATH
cd /home/xiong/projects/YX2602_FAULTRECOG
mvn -DskipTests clean package
```

重启日志采集器后，确认当前应用汇总文件仍在更新。当前入口默认读取 `logs/heartbeat-monitor.log`，请用 `readlink -f` 确认它指向当前应用；如为旧的同名普通文件或希望固定应用，使用下面的 `--input` 指定。

## 简洁统计：最近60分钟的平均和最高吞吐

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/report-heartbeat-performance.sh --simple
```

默认统计最近60分钟。`--simple`直接在终端打印消费、解析、Ignite写入的平均值和最高值，同时覆盖原报告文件。以下数值只是格式示例：

```text
消费平均：10000.000 条原始消息/秒（合计 36000000 条，覆盖 3600.000 秒，子任务 5/5）；最高：12000.000 条原始消息/秒（约30.000秒采样窗口）
解析平均：12000.000 条单梯消息/秒（合计 43200000 条，覆盖 3600.000 秒，子任务 5/5）；最高：15000.000 条单梯消息/秒（约30.000秒采样窗口）
Ignite写入平均：8000.000 条单梯更新/秒（合计 28800000 条，覆盖 3600.000 秒，子任务 5/5）；最高：10000.000 条单梯更新/秒（约30.000秒采样窗口）
```

完整覆盖60分钟时，平均每秒条数为该阶段全部Topic及子任务的总条数÷3600秒，不再除以并行度。日志只覆盖部分时间时按实际样本时间跨度计算，并提示不足60分钟；子任务不齐时提示不能代表全作业。无近期日志会标记缺失/陈旧，不用旧数据或0冒充当前指标。

最高值从同一阶段对齐后的完整采样窗口中选取：每个窗口先合计全部子任务的速率，再取最高值。采样周期默认约30秒，报告显示实际中位采样周期；不是单秒瞬时最大值，不把各子任务不同时刻的最高值相加。对齐容许小幅定时抖动，属于摘要近似统计。缺少任一配置子任务、同一路窗口歧义或明显偏离正常周期的短结束摘要/延迟摘要不参与最高值；没有完整窗口时显示“—”，而不是0。

消费取validation.input，解析取split.parsed，写入取ignite_sink.committed；数据库skipped不计入有效写入。消费单位是原始消息，解析与写入单位是单梯消息/更新，拆分与过滤会使三个数字不同。

持续刷新简洁输出：

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/report-heartbeat-performance.sh --simple --watch
```

每30秒重新统计最近60分钟，按Ctrl+C停止；不是启动后等待满60分钟才输出。显式 `--minutes` 优先，例如 `--simple --minutes 10` 统计最近10分钟。简洁模式和完整报告默认均为60分钟；旧启动命令若明确写了 `--minutes 15`，仍会按15分钟运行，应改为60或省略该参数。

60分钟均值/峰值版本经Java 11干净构建验证，13项统计测试及4项独立JAR运行测试通过。覆盖并行窗口峰值汇总、分时峰值不相加、同号并行样本跨时钟边界对齐、短/缺失窗口排除、0与无样本区分、默认60分钟与显式参数优先，以及一次运行和持续模式。

## 一次统计最近60分钟

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/report-heartbeat-performance.sh --minutes 60
cat /home/xiong/projects/YX2602_FAULTRECOG/logs/heartbeat-performance.md
```

默认报告位置为 `/home/xiong/projects/YX2602_FAULTRECOG/logs/heartbeat-performance.md`。这是按执行时当前时钟回看60分钟的统计；日志停止更新时会提示陈旧，超过窗口无样本时不会用旧数据充当当前吞吐。

固定当前应用日志时使用实际ID：

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/report-heartbeat-performance.sh \
  --input /home/xiong/projects/YX2602_FAULTRECOG/logs/heartbeat-monitor-application_实际ID.log \
  --minutes 60
```

## 持续监控

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/report-heartbeat-performance.sh --minutes 60 --watch
```

每30秒重新读取日志并替换报告，按Ctrl+C停止。关闭终端后仍要运行时，可自行使用nohup；持续状态输出可重定向到/dev/null，避免额外生成无限增长的控制台日志：

```bash
nohup bash /home/xiong/projects/YX2602_FAULTRECOG/bin/report-heartbeat-performance.sh \
  --minutes 60 --watch > /dev/null 2>&1 < /dev/null &
```

查看持续统计进程：

```bash
ps -eo pid,args | grep '[c]om.hitachi.yx2602.heartbeat.HeartbeatPerformanceReport'
```

统计进程与HeartbeatLogCollector是两个独立进程：前者汇总指标，后者从Flink下载监控事件。不要为重启统计进程停止Flink。建议同一输出路径只运行一个统计进程；脚本不管理PID和自动去重启动。

## 历史回看和归档文件

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/report-heartbeat-performance.sh \
  --input /实际路径/历史监控.log \
  --as-of-latest --minutes 60 \
  --output /实际路径/历史性能报告.md
```

`--as-of-latest`把统计窗口结束时间设为最后性能摘要时间，报告明确标记历史模式，不代表当前运行。`--input`可重复，适合跨文件分段分析：

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/report-heartbeat-performance.sh \
  --input /实际路径/上一段.log \
  --input /实际路径/当前段.log \
  --minutes 60
```

只读取当前文件时，轮转或跨日后该文件可能不足60分钟；工具报告实际样本跨度。需要完整区间时明确传入相邻分段。普通TaskManager日志也可读取（无时区时间戳按北京时间解释）。同事件在独立监控、普通日志或重复输入中的副本按毫秒时间和指标内容去重；不合并不同job或不同attempt。

## 统计内容与口径

| 部分 | 指标 |
| --- | --- |
| Kafka读取/校验 | 原始消息读取/秒、校验通过/秒、拒绝数、Kafka时间戳及CreatedDate年龄、最后观察到的分区offset |
| 解析 | 原始输入/秒、解析得到单梯/秒、计算耗时、端口拒绝数、10分钟过期数及比例 |
| 新旧判断 | 单梯输入/接受速率、重复和旧版本数、计算耗时及向Sink输出等待 |
| Ignite写入 | 有效更新/秒、数据库跳过数、批次大小与耗时、逐梯读取/putAll/commit耗时及占批次累计耗时比例 |
| 子任务比较 | 每路吞吐、最新队列/容量、平均队列、满队列采样次数、写调用占采样时间、最后摘要时间及writer_failed |
| 检查点 | 完成/中止通知涉及的ID数、可配对的Sink刷写等待均值和最大值 |

吞吐分母为该阶段最早样本窗口开始到最后样本结束的实际时间跨度。分子为全部已覆盖子任务计数之和，不再除以并行度；Source只有3个Kafka分区时，其余Source子任务空闲正常，不影响按5路统计Sink。样本断档/子任务缺失会低估全作业吞吐，工具会报告子任务覆盖；各路Sink最后摘要时间可检查单路是否停止更新。

耗时均值按total_ms之和除以samples之和计算，避免简单平均不同调用量的avg_ms。没有样本显示“—”；日志中的历史最大值不能推导P95/P99。写入时间含网络、锁和等待，不是CPU占用。各阶段数据单位不同，不能用Source计数减Sink计数推断丢失。数据库skipped不计为有效入库，批次年龄包含被跳过记录。

自动选取输入中最后性能摘要所属job，只统计该job；如窗口内出现多个attempt，各阶段各子任务仅保留最新attempt并标明排除量，避免恢复前后重复累计。窗口选择按摘要结束时间，近似统计的边界可能覆盖请求范围之前的部分窗口。缺失、陈旧、损坏或末尾未完成的日志行不应被解释成零耗时或健康。

报告中的完成通知数不是完整检查点成功率，刷写等待不是完整检查点耗时。精确Kafka LAG需要另采Broker末尾、Source当前位点和消费者组提交位点；本工具不宣称计算真实LAG，也不能直接判断Ignite节点CPU、GC、网络、淘汰或磁盘根因。

多Topic消费后，位点展示为Topic/分区，识别 `observed_offset_<topic>_pN`，也兼容旧字段 `observed_offset_pN`（标注未注明Topic）。吞吐为所有订阅Topic合计，不把不同Topic同号分区的offset覆盖或相减。

## 本地历史样例

当前本地可用日志最后记录为2026-11-10 10:40，属于旧作业样本；2026-11-13新增工具时无法据此判断内网当前作业性能。历史样例报告生成于 `.runtime/heartbeat-performance-20261110-sample.md`，供核对字段和统计口径。

样例最近15分钟的全作业读取约9700条原始消息/秒，解析约10058条单梯/秒，有效入库约4702条单梯/秒；校验平均0.029ms、分梯计算0.043ms、单批写入184.524ms，读取最新状态累计占批次约58.1%。这些结果仅对应报告标记的历史区间，与先前按10:25整分钟起点取样的范围略有差别。

本次新增7项统计口径测试及2项独立JAR入口测试均通过，覆盖并行吞吐汇总、耗时加权、重复副本去重、旧日志/历史模式、job与attempt隔离、子任务缺失、检查点配对、断档时间分母、输入保护，以及一次运行/持续模式启动。未生成或更新部署包；未连接或修改内网服务。
