package com.hitachi.yx2602.heartbeat;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Paths;

/** Runs reader and parser; production must still attach the persistence module. */
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
        System.out.println("PREVIEW模式：读取并解析心跳，打印单梯JSON且会推进消费位点；尚未写入Ignite3。");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        HeartbeatReader.configureEnvironment(env, config);
        HeartbeatParser.build(HeartbeatReader.build(env, config), config)
                .print("heartbeat-parsed-preview").uid("heartbeat-parsed-preview-v1");
        env.execute(config.jobName);
    }
}
