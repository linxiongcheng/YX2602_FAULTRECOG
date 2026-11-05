# 监控日志采集器缺失轮转文件修复

2026-11-05现场报错：`The file heartbeat-monitor.log.1 does not exist on the TaskExecutor`，调用栈位于 `TaskManagerCustomLogHandler`，属于Flink REST下载日志失败。这些错误本身不证明Kafka消费或Ignite写入失败，应另外检查Flink作业状态、检查点和心跳表更新。

`.log.1` 是 `heartbeat-monitor.log` 达到轮转大小后才生成的上一份文件。旧采集器每轮固定请求两份文件，而Flink 1.20.5对缺失文件返回500；连续10轮采集失败后汇总进程退出。新采集器先读取各TaskManager文件列表，只下载实际存在的文件，每轮重新发现，保留先旧后新及事件去重规则。真正的HTTP错误继续报错重试。

## 当前作业不停机更新

本次只修改提交节点的日志采集器。无需停止Flink、重启Ignite、删除表或改变Kafka消费组。已运行作业使用YARN提交时上传的JAR副本，提交节点的新JAR用于重启采集器和下次提交。

1. 在提交节点查看当前采集器进程，核对命令行中的应用ID：

   ```bash
   ps -eo pid,args | grep '[c]om.hitachi.yx2602.heartbeat.HeartbeatLogCollector'
   ```

2. 若该应用的旧采集器仍在运行，使用 `kill 实际采集器PID` 停止它。PID必须来自上一步、属于HeartbeatLogCollector，不能使用YARN/Flink容器PID。若已自行退出则跳过。等待该进程退出后再替换文件。

3. 将修复包中的 `target/heartbeat-reader-0.1.0.jar` 及校验文件上传并替换提交节点 `/home/xiong/projects/YX2602_FAULTRECOG/target/` 下对应文件。保留服务器自己的 `local.properties`，也保留 `logs` 下的日志、offsets和lock文件。不要只删除lock文件绕过正在运行的采集器。

4. 使用当前应用ID及REST地址重新启动汇总，地址取原提交输出的 `Found Web Interface` 或YARN的Tracking URL；提交节点必须能访问：

   ```bash
   bash /home/xiong/projects/YX2602_FAULTRECOG/bin/collect-heartbeat-logs.sh \
     application_实际ID http://实际Flink主机:实际REST端口
   ```

   不执行 `start-heartbeat.sh`，以免重复提交心跳作业。

5. 观察本次新产生的状态及监控内容：

   ```bash
   tail -n 30 /home/xiong/projects/YX2602_FAULTRECOG/logs/collector-application_实际ID.log
   tail -n 20 /home/xiong/projects/YX2602_FAULTRECOG/logs/heartbeat-monitor-application_实际ID.log
   ```

   每30秒一轮，正常输出 `collected=N`；0表示本轮没有新增完整事件，不等于业务没有数据。旧报错保留在历史日志中，需要按更新时间判断。

## 验证

先用真实Flink 1.20.5 MiniCluster在没有 `.log.1` 的情况下复现HTTP 500，再验证修复后只下载已有文件。覆盖有/无轮转文件、启动时两份均不存在、后续新文件发现、轮转顺序与去重、真实HTTP失败继续报错，以及独立JAR采集入口。

Java 11执行 `mvn -o -B -ntp clean verify`，193项测试通过，无失败、错误或跳过。集群尚未应用此修复；部署后需观察当前采集器和作业状态。
