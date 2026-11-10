# DolphinScheduler Flink节点部署配置汇总

整理日期：2026-11-10。适用于YX2602心跳读取、解析及Ignite写入程序。

本方案使用DolphinScheduler原生 **Flink任务节点**，不切换成Shell任务。以下依据本次任务表单、生成的提交命令及异常日志整理；DolphinScheduler具体版本未提供。

## 1. 当前状态和部署方式

已确认：Worker能够执行Flink 1.20.5；主程序JAR能够加载；修正资源路径后程序能够读取local.properties，并通过Ignite连接和表结构检查；任务实际保存的flinkVersion为 `>=1.10`。

此前YARN JobManager使用Java 8，加载Java 11编译的类时出现UnsupportedClassVersionError并退出，随后客户端访问REST端口被拒绝。修改config.yaml过程中又出现重复顶层env键的解析错误。本页给出合并后的配置方式。**目前尚未收到修改完成后，JobManager实际使用Java 11且作业持续RUNNING的成功日志，不能将以下整理视为已完成集群验收。**

| 项目 | 当前DolphinScheduler Flink节点 | 原bin/start-heartbeat.sh |
| --- | --- | --- |
| 提交形式 | `flink run -m yarn-cluster ...` | `flink run-application -t yarn-application ...` |
| 业务main执行位置 | 提交客户端，即Worker进程所在环境 | YARN Application模式的JobManager |
| main读取的配置路径 | Worker任务目录中的资源相对路径 | YARN分发后的local.properties文件名 |
| 本文是否采用 | 是 | 仅作配置参考，不由本任务调用 |

Flink 1.20.5对Per-Job模式有弃用提示，但当前日志已经显示可以部署YARN应用。表单的cluster不等同于yarn-application，不在选项参数中添加run-application子命令。

## 2. Worker与YARN节点准备

截图中的Worker地址为 `10.96.129.33:1234`，具体主机名和Worker分组以实际平台为准。

| 内容 | 路径或要求 |
| --- | --- |
| Worker上的Flink安装目录 | `/opt/flink-1.20.5` |
| Worker实际加载的Flink配置文件 | `/opt/flink-1.20.5/conf/config.yaml`，客户端日志已确认 |
| 本应用的Java 11目录 | `/opt/java11` |
| Worker上的Hadoop配置目录 | `/etc/hadoop/conf`，按本次生成命令中的设置 |
| Hadoop命令 | Worker环境能够执行hadoop，`hadoop classpath`能返回所需依赖 |
| 各YARN容器所在节点 | 均须有可执行的 `/opt/java11/bin/java` |

只给Worker安装Java 11不能保证YARN JobManager和TaskManager使用Java 11。若Worker运行在容器中，安装目录和Hadoop配置必须在Worker容器内可见；修改宿主机同名文件不一定影响容器。

原脚本使用的 `/opt/flink-1.20.5/conf/cdh-env.sh` 为原部署环境的自定义文件，不是官方Flink安装包保证包含的文件。本次DolphinScheduler环境已直接配置Hadoop，不引用缺失的cdh-env.sh。

## 3. DolphinScheduler任务环境

通常在“安全中心→环境管理”新增或编辑环境，命名可用 `flink-1.20.5-java11`，配置如下。菜单名称及权限以实际版本为准。

```bash
export FLINK_HOME=/opt/flink-1.20.5
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_CLASSPATH="$(hadoop classpath)"
export JAVA_HOME=/opt/java11
export PATH="$JAVA_HOME/bin:$FLINK_HOME/bin:$PATH"
```

上述内容是平台的环境配置，不是Shell任务启动脚本。hadoop命令需要已在Worker基础PATH中可用；如果安装位置不同，先调整为实际路径。该顺序沿用已观察到的Worker提交环境，不要求修改CDH或DolphinScheduler自身的全局JDK。

保存后，在Flink任务公共配置中的“环境名称/环境配置”选择此环境，并选择已准备这些依赖的Worker分组。多Worker分组需确保可能接单的Worker具有一致的安装路径及Flink配置。

## 4. 资源中心与文件路径

本次资源路径如下，保持大小写一致：

| 资源 | 资源中心完整名称 | 任务执行目录中的相对路径 |
| --- | --- | --- |
| 主程序 | `/FlinkProjectsJar/YX2602_FAULTRECOG/heartbeat-reader-0.1.0.jar` | `FlinkProjectsJar/YX2602_FAULTRECOG/heartbeat-reader-0.1.0.jar` |
| 业务配置 | `/FlinkProjectsJar/YX2602_FAULTRECOG/local.properties` | `FlinkProjectsJar/YX2602_FAULTRECOG/local.properties` |

主程序包下拉框选择JAR；任务下方的“资源”选择local.properties。资源中心的根斜杠不是Worker文件系统根目录，因此主程序参数中使用相对路径，不带开头的 `/`。以后若移动资源目录，需同步更新程序参数和yarn.ship-files。

## 5. Flink节点表单填写

| 字段 | 当前配置 |
| --- | --- |
| 程序类型 | JAVA |
| 主函数的Class | `com.hitachi.yx2602.heartbeat.HeartbeatReaderJob` |
| 主程序包 | 选择上述heartbeat-reader-0.1.0.jar资源 |
| 部署方式 | cluster |
| Flink版本 | `>=1.10` |
| 任务名称/appName | `yx2602-heartbeat-storage` |
| JobManager内存 | 1G |
| TaskManager内存 | 2G |
| Slot数量 | 2，每个TaskManager |
| TaskManager数量 | 表单保存值3；当前版本分支的已生成命令未输出旧参数-yn，不据此保证固定启动3个TaskManager |
| 并行度 | 5 |
| YARN队列 | default，已生成命令包含 `-yqu default` |
| 资源 | 选择上述local.properties |
| 自定义参数 | 当前无额外需要，可留空 |

项目代码还会调用 `env.setParallelism(config.parallelism)`。业务配置local.properties必须同步设置 `parallelism=5`，不要只修改表单的并行度；parser.max.parallelism沿用既有项目配置。

已生成命令中的 `-ynm` 是YARN应用名称参数，与之前导致JAR解析失败的独立 `-yn` 参数不同，不要误删。

### 主程序参数

按一行填写：

```text
--config FlinkProjectsJar/YX2602_FAULTRECOG/local.properties --run
```

### 选项参数

下面是单条逻辑行，整体复制，不手工插入换行、不加行末反斜杠，不复制Markdown代码块标记。文本框自动折行不影响。

```text
-Denv.java.home=/opt/java11 -Dcontainerized.master.env.JAVA_HOME=/opt/java11 -Dcontainerized.taskmanager.env.JAVA_HOME=/opt/java11 '-Denv.java.opts.all=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED' -Dyarn.application.name=yx2602-heartbeat-storage -Dclassloader.parent-first-patterns.additional=org.apache.kafka. -Dyarn.ship-files=FlinkProjectsJar/YX2602_FAULTRECOG/local.properties
```

原记录中的三项Java路径参数保留，同时按下一节设置Worker实际加载的config.yaml。此前已有命令携带Java 11参数而JobManager仍运行Java 8，因此填写参数本身不能代替新应用的运行验证。

`yarn.ship-files`负责将Worker侧文件分发给YARN，不会先把Worker子目录中的文件复制到当前目录。本次Per-Job模式main先在Worker中运行，所以 `--config` 同样需要带资源子目录；不要混用原Application模式脚本中的 `--config local.properties`。

## 6. config.yaml中的Java 11配置

编辑 **实际执行该任务的Worker内部** 的文件：

```text
/opt/flink-1.20.5/conf/config.yaml
```

以下为Java相关配置片段，不是替换整份配置文件的完整模板。保留已有jobmanager、taskmanager、rest和其他配置。已有env、java或containerized块时合并，不能重复创建同名键。

```yaml
env:
  java:
    home: /opt/java11
    opts:
      all: >-
        --add-opens=java.base/java.nio=ALL-UNNAMED
        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
        --add-opens=java.base/java.lang=ALL-UNNAMED
        --add-opens=java.base/java.util=ALL-UNNAMED
        --add-opens=java.base/java.io=ALL-UNNAMED

containerized:
  master:
    env:
      JAVA_HOME: /opt/java11
  taskmanager:
    env:
      JAVA_HOME: /opt/java11
```

- env.java.home指定本应用的JDK路径；containerized下的JAVA_HOME设置YARN容器环境。
- opts.all包含当前Ignite客户端所用模块访问选项。若原文件还有其他经过确认需要的JVM参数，合并到同一个all值，不创建第二个all或opts。
- 本安装包原env.java.opts.all注释说明其默认参数主要用于Java 17；当前选择Java 11。不要因此删除整个config.yaml的其他部分。
- YAML使用空格缩进，无行末反斜杠。`>-`会将多行模块参数折叠成一行字符串。
- 上面的env、containerized均为顶层键；java下的home与opts同级；master/taskmanager下面的env是嵌套键，名称相同但不构成顶层env重复。
- 修改配置后需新提交应用，已运行的YARN进程不会自动换JDK；无需为读取新的Flink配置重启DolphinScheduler服务。

配置文件和选项参数中的Java设置应保持一致。若Worker设置了FLINK_CONF_DIR，以实际加载目录为准；客户端日志中应能看到从哪个config.yaml读取配置。

## 7. 提交前检查与运行验收

在Worker实际环境检查，以下命令仅用于诊断，不要求使用Shell任务启动业务：

```bash
/opt/java11/bin/java -version
command -v flink
/opt/flink-1.20.5/bin/flink --version
grep -n '^env:' /opt/flink-1.20.5/conf/config.yaml
grep -n '^containerized:' /opt/flink-1.20.5/conf/config.yaml
```

顶层env和containerized各应只有一处，flink --version不应再报YAML解析错误。各YARN计算节点也分别检查 `/opt/java11/bin/java -version`。Worker租户需可读资源及Flink配置，并具备当前Kafka、HDFS、YARN和Ignite访问权限。

然后在DolphinScheduler手动执行一次：

1. 核对任务日志的环境导出、资源路径及完整flink task command；flinkVersion实际值为>=1.10，不再生成导致JAR解析错误的-yn。
2. 确认配置加载、Ignite检查通过。程序只有进入正式流作业阶段才会持续处理心跳。
3. 记录新Application ID和JobID，检查YARN应用及Flink作业RUNNING。
4. 检查 **新应用的JobManager和TaskManager** 启动日志，确认Java 11，而非仅查看Worker的java -version。不得再出现只支持class file version 52的异常。
5. 确认5路Sink、Checkpoint持续成功、心跳表随有效新数据更新。提交成功或有Application ID不等于业务已成功运行。

该程序为常驻流任务，节点在附着等待作业结果时长期RUNNING可以是正常状态。不设置每隔数分钟重复提交；首次联调建议任务自动重试为0，重新执行前核对旧应用状态，避免并发重复运行。

本次Flink节点不会调用bin/start-heartbeat.sh，因此也不会自动执行其末尾的监控日志采集器启动步骤。若需要固定目录的汇总日志，采集器需单独管理；不影响Flink/YARN原始日志和作业本身运行。

## 8. 日志位置与已遇到的问题

| 日志 | 当前位置或入口 |
| --- | --- |
| DolphinScheduler任务日志 | 页面日志；Worker内 `/opt/dolphinscheduler/worker-server/logs/日期/任务日志文件.log` |
| 生成的任务命令 | 日志中显示的 `/tmp/dolphinscheduler/exec/process/.../*.command`，临时目录可能被清理 |
| Flink提交客户端日志 | 默认 `/opt/flink-1.20.5/log/*client*.log`，若设置FLINK_LOG_DIR则按实际值 |
| YARN AM/JobManager/TaskManager日志 | YARN/Flink页面或 `yarn logs -applicationId application_实际ID` |

| 报错或现象 | 已确认含义/处理 |
| --- | --- |
| flink: 未找到命令 | Worker未准备安装目录或PATH；核对实际Worker/容器环境 |
| cdh-env.sh: 没有那个文件或目录 | 引用了原手动提交机的自定义脚本；当前环境改用已核实的Hadoop路径 |
| 每行-D参数分别提示未找到命令 | 选项参数含真实换行，被拆成多条命令；表单中改为单行 |
| JAR file does not exist: -yn | 旧提交参数被当成JAR；核对实际flinkVersion和完整命令，而不是重新上传JAR |
| UI显示<1.10，任务JSON为>=1.10 | 本次已观察到的回显与实际保存不一致，以任务参数及生成命令核对 |
| NoSuchFileException: local.properties | main在Worker读取时路径错误；使用资源子目录相对路径 |
| UnsupportedClassVersionError，55与52 | 类按Java 11编译，实际JVM为Java 8；修正YARN容器JDK并新提交 |
| REST连接10.98.254.36:43282被拒绝 | 历史应用中已结合日志确认JobManager因Java版本错误退出，REST随之关闭；其他应用仍需分别核对 |
| exitStatusCode:31 | 退出码本身不能定位原因；本次客户端日志确认是YAML重复顶层env |
| found duplicate key env | 合并原有env.java块，保留一个顶层env；不要追加第二块 |
| SLF4J multiple bindings | 本次同时加载Flink和CDH日志绑定的警告，不是前述异常的已确认根因 |

出错后优先读取最底层Caused by、Flink客户端文件日志及YARN容器日志，不通过关闭类加载检查等方式掩盖Java版本或配置解析问题。
