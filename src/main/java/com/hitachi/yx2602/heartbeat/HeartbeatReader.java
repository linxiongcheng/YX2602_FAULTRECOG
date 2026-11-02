package com.hitachi.yx2602.heartbeat;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/** Builds one common checkpointed graph; the caller owns the valid downstream and env.execute(). */
public final class HeartbeatReader {
    private HeartbeatReader() { }

    public static void configureEnvironment(StreamExecutionEnvironment env, ReaderConfig config) {
        env.setParallelism(config.parallelism);
        env.enableCheckpointing(config.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpoint = env.getCheckpointConfig();
        checkpoint.setCheckpointTimeout(config.checkpointTimeoutMs);
        checkpoint.setMinPauseBetweenCheckpoints(1000);
        checkpoint.setMaxConcurrentCheckpoints(1);
        checkpoint.setTolerableCheckpointFailureNumber(0);
        checkpoint.setCheckpointStorage(config.checkpointStorage);
        checkpoint.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(config.restartAttempts,
                Time.milliseconds(config.restartDelayMs)));
    }

    public static SingleOutputStreamOperator<HeartbeatMessage> build(
            StreamExecutionEnvironment env, ReaderConfig config) throws Exception {
        if (!env.getCheckpointConfig().isCheckpointingEnabled()) {
            throw new IllegalStateException("必须先启用检查点，才能保证异常消息至少一次交付");
        }
        OffsetBootstrap.initialize(config);
        return createGraph(env, config);
    }

    static SingleOutputStreamOperator<HeartbeatMessage> createGraph(
            StreamExecutionEnvironment env, ReaderConfig config) {
        KafkaSource<RawHeartbeat> source = KafkaSource.<RawHeartbeat>builder()
                .setBootstrapServers(config.bootstrapServers)
                .setTopics(config.inputTopic)
                .setGroupId(config.groupId)
                .setProperties(config.consumerProperties())
                // Bootstrap initialized absent offsets to latest. Restored checkpoint wins.
                // Missing/expired offsets now fail instead of silently skipping unprocessed data.
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.NONE))
                .setDeserializer(new HeartbeatDeserializer())
                .build();
        SingleOutputStreamOperator<HeartbeatMessage> valid = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "heartbeat-kafka-source")
                .uid("heartbeat-kafka-source-v1")
                .process(new ValidateHeartbeat(config.futureSkewMs))
                .name("heartbeat-validation").uid("heartbeat-validation-v1");
        attachErrorSink(valid.getSideOutput(ValidateHeartbeat.ERRORS), config,
                "heartbeat-error-kafka", "heartbeat-error-kafka-v1");
        return valid;
    }

    static void attachErrorSink(DataStream<String> errors, ReaderConfig config, String name, String uid) {
        KafkaSink<String> errorSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.bootstrapServers)
                .setKafkaProducerConfig(config.producerProperties())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.errorTopic)
                        .setValueSerializationSchema(new SimpleStringSchema()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        // Native sink flushes pending sends and propagates async failures before checkpoint ACK.
        errors.sinkTo(errorSink).name(name).uid(uid);
    }
}
