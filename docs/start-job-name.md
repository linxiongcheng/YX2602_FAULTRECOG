# 启动时指定作业名称

`start-heartbeat.sh` 新增可选参数 `--job-name NAME`。传入时，本次提交的Flink作业名和YARN应用名同时使用该名称，不修改local.properties。

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/start-heartbeat.sh \
  --job-name yx2602-heartbeat-production
```

名称有空格或中文时加引号；不允许空名称、首尾空白、换行或以 `-` 开头。参数按原样作为独立命令行参数传递，不将名称当作shell代码执行。

从保存点恢复并指定名称，两个选项的顺序不限：

```bash
bash /home/xiong/projects/YX2602_FAULTRECOG/bin/start-heartbeat.sh \
  --job-name yx2602-heartbeat-production \
  --from-savepoint hdfs:///flink/savepoints/实际保存点路径
```

原有 `-s PATH` 仍兼容。不传 `--job-name` 时保持旧行为：YARN应用名为yx2602-heartbeat-storage，Flink作业名取local.properties中的job.name。

本功能需要同步更新脚本和HeartbeatReaderJob.java，并在内网使用Java 11重新编译。直接调用Java入口时，名称参数放在运行模式后面：

```text
--config local.properties --run --job-name yx2602-heartbeat-production
```

Java入口只在 `--run` 或 `--preview` 下接受该参数，其他检查/配置发布模式保留原命令格式。

名称只在新提交时生效，不能热修改已运行作业。仅改名时不更换Kafka消费组、不修改Topic或检查点目录，不改变算子UID；需要保留处理进度时按现有Savepoint流程停止旧作业，再恢复提交。启动脚本每次运行都会提交新应用，不会自动替换或停止同名/旧名作业。
