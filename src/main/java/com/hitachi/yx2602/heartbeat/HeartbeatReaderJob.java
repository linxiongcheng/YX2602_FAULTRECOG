package com.hitachi.yx2602.heartbeat;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Paths;

/** 作业入口：校验配置、检查 Ignite、预览数据或启动正式写入，具体模式由命令行指定。 */
public final class HeartbeatReaderJob {
    private HeartbeatReaderJob() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 3 || !args[0].equals("--config")
                || !(args[2].equals("--check-config") || args[2].equals("--preview")
                || args[2].equals("--check-ignite") || args[2].equals("--run")
                || args[2].equals("--watch-history-config") || args[2].equals("--publish-history-config"))) {
            throw new IllegalArgumentException(
                    "用法: --config /path/reader.properties --check-config|--check-ignite|--preview|--run|--watch-history-config|--publish-history-config");
        }
        if (args[2].equals("--watch-history-config") || args[2].equals("--publish-history-config")) {
            try (IgniteConfig.HistoryPublisher publisher = new IgniteConfig.HistoryPublisher(Paths.get(args[1]))) {
                if (args[2].equals("--watch-history-config")) publisher.watch();
                else publisher.sync();
            }
            return;
        }
        ReaderConfig config = ReaderConfig.load(Paths.get(args[1]));
        if (args[2].equals("--check-config")) {
            System.out.println("配置结构校验通过；未连接Kafka/Ignite或验证检查点存储权限。正式运行还要求ignite.addresses。");
            return;
        }
        boolean production = args[2].equals("--run");
        if (production || args[2].equals("--check-ignite")) {
            // 先检查数据库连接和表结构，避免检查失败时已经初始化 Kafka 消费位点。
            try (IgniteHeartbeatStore ignored = new IgniteHeartbeatStore(config.ignite, production)) {
                System.out.println("Ignite连接与缓存结构校验通过；服务端heartbeat_memory区域须配置persistenceEnabled=false，客户端无法验证该开关。");
            }
            if (!production) return;
        }
        System.out.println(production ? "RUN模式：读取、解析并事务写入Ignite 2.18.0。"
                : "PREVIEW模式：读取并解析心跳，打印单梯JSON且会推进消费位点；不写入Ignite。");
        Pipeline.execute(config, production);
    }

    // 配置同步入口不初始化流处理类；只有正式运行或预览时才加载Flink作业。
    private static final class Pipeline {
        static void execute(ReaderConfig config, boolean production) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            HeartbeatReader.configureEnvironment(env, config);
            org.apache.flink.streaming.api.datastream.DataStream<ElevatorUpdate> updates =
                    HeartbeatParser.build(HeartbeatReader.build(env, config), config);
            if (production) IgniteHeartbeatSink.attach(updates, config.ignite);
            else updates.print("heartbeat-parsed-preview").uid("heartbeat-parsed-preview-v1");
            env.execute(config.jobName);
        }
    }
}
