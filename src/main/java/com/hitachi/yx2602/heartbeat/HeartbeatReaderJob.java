package com.hitachi.yx2602.heartbeat;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Paths;

/** Explicit preview only: production must attach the real parsing and persistence modules. */
public final class HeartbeatReaderJob {
    private HeartbeatReaderJob() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 3 || !args[0].equals("--config")
                || !(args[2].equals("--check-config") || args[2].equals("--preview"))) {
            throw new IllegalArgumentException(
                    "用法: --config /path/reader.properties --check-config|--preview");
        }
        ReaderConfig config = ReaderConfig.load(Paths.get(args[1]));
        if (args[2].equals("--check-config")) {
            System.out.println("配置结构校验通过；未连接Kafka或验证检查点存储权限。");
            return;
        }
        System.out.println("PREVIEW模式：有效消息仅打印元数据且会推进消费位点，请使用独立测试消费组与Topic。");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        HeartbeatReader.configureEnvironment(env, config);
        HeartbeatReader.build(env, config).print("heartbeat-preview").uid("heartbeat-preview-v1");
        env.execute(config.jobName);
    }
}
