package com.hitachi.yx2602.heartbeat;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** 读取模块：配置检查点、初始化 Kafka 位点并分流合法消息与异常消息，由入口启动作业。 */
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
        initializeOffsets(config);
        return createGraph(env, config);
    }

    static SingleOutputStreamOperator<HeartbeatMessage> createGraph(
            StreamExecutionEnvironment env, ReaderConfig config) {
        KafkaSource<RawHeartbeat> source = KafkaSource.<RawHeartbeat>builder()
                .setBootstrapServers(config.bootstrapServers)
                .setTopics(config.inputTopics)
                .setGroupId(config.groupId)
                .setProperties(config.consumerProperties())
                // 首次缺失的位点已初始化到分区末尾；从检查点恢复时优先使用检查点位点。
                // 此处发现位点缺失或失效就报错，防止自动跳过尚未处理的数据。
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.NONE))
                .setDeserializer(new Deserializer())
                .build();
        SingleOutputStreamOperator<HeartbeatMessage> valid = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "heartbeat-kafka-source")
                .uid("heartbeat-kafka-source-v1")
                .process(new ValidationFunction(config.futureSkewMs, config.monitorLogIntervalMs))
                .name("heartbeat-validation").uid("heartbeat-validation-v1");
        attachErrorSink(valid.getSideOutput(ValidationFunction.ERRORS), config,
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
        // 检查点确认前等待异常消息发送完成，并传播异步发送失败；恢复重放时异常消息可能重复。
        errors.sinkTo(errorSink).name(name).uid(uid);
    }

    private static void initializeOffsets(ReaderConfig config) throws Exception {
        Properties properties = config.connectionProperties();
        properties.setProperty("bootstrap.servers", config.bootstrapServers);
        properties.setProperty("request.timeout.ms", "30000");
        properties.setProperty("default.api.timeout.ms", "60000");
        try (AdminClient admin = AdminClient.create(properties)) {
            // 输入和异常 Topic 都须预先创建，启动时检查名称和可访问性。
            Set<String> topicNames = new java.util.LinkedHashSet<>(config.inputTopics);
            topicNames.add(config.errorTopic);
            Map<String, TopicDescription> topics = admin.describeTopics(topicNames)
                    .allTopicNames().get(60, TimeUnit.SECONDS);
            Set<TopicPartition> partitions = inputPartitions(config, topics);
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(config.groupId)
                    .partitionsToOffsetAndMetadata().get(60, TimeUnit.SECONDS);
            Set<TopicPartition> missing = missingPartitions(partitions, committed);
            if (missing.isEmpty()) return;
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            for (TopicPartition partition : missing) request.put(partition, OffsetSpec.latest());
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = admin.listOffsets(request)
                    .all().get(60, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetAndMetadata> initial = new HashMap<>();
            latest.forEach((partition, offset) -> initial.put(partition,
                    new OffsetAndMetadata(offset.offset(), "yx2602-initial-latest")));
            // 只补齐缺失位点，表示本次开始消费的位置，不代表这些历史数据已处理。
            // 同一消费组只允许一个作业提交或运行，避免并发初始化互相覆盖。
            admin.alterConsumerGroupOffsets(config.groupId, initial).all().get(60, TimeUnit.SECONDS);
        }
    }

    static Set<TopicPartition> missingPartitions(Set<TopicPartition> partitions,
                                                Map<TopicPartition, OffsetAndMetadata> committed) {
        return partitions.stream().filter(partition -> committed.get(partition) == null)
                .collect(Collectors.toSet());
    }

    static Set<TopicPartition> inputPartitions(ReaderConfig config, Map<String, TopicDescription> topics) {
        return config.inputTopics.stream().flatMap(name -> topics.get(name).partitions().stream()
                .map(partition -> new TopicPartition(name, partition.partition()))).collect(Collectors.toSet());
    }

    /** 保留原始字节和 Kafka 来源信息，非法编码也能进入异常 Topic 供排查。 */
    static final class Deserializer implements KafkaRecordDeserializationSchema<RawHeartbeat> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawHeartbeat> out) {
            out.collect(new RawHeartbeat(record.value(), record.key(), record.topic(),
                    record.partition(), record.offset(), record.timestamp()));
        }

        @Override
        public TypeInformation<RawHeartbeat> getProducedType() {
            return TypeInformation.of(RawHeartbeat.class);
        }
    }

    /** 在 Flink 算子中执行整条消息校验，并按错误原因计数和输出到侧流。 */
    static final class ValidationFunction extends ProcessFunction<RawHeartbeat, HeartbeatMessage> {
        private static final long serialVersionUID = 1L;
        public static final OutputTag<String> ERRORS = new OutputTag<String>("heartbeat-errors-v1") { };
        private final long futureSkewMs;
        private final long monitorIntervalMs;
        private transient HeartbeatDiagnostics diagnostics;
        private transient HeartbeatValidator validator;
        private transient Counter accepted, rejected;
        private transient Map<String, Counter> reasons;

        public ValidationFunction(long futureSkewMs) { this(futureSkewMs, 30000); }
        public ValidationFunction(long futureSkewMs, long monitorIntervalMs) {
            this.futureSkewMs = futureSkewMs; this.monitorIntervalMs = monitorIntervalMs;
        }

        @Override
        public void open(OpenContext context) {
            validator = new HeartbeatValidator(futureSkewMs);
            diagnostics = HeartbeatDiagnostics.forTask("validation", getRuntimeContext(), monitorIntervalMs);
            diagnostics.start();
            MetricGroup metrics = getRuntimeContext().getMetricGroup().addGroup("heartbeat_reader");
            accepted = metrics.counter("accepted");
            rejected = metrics.counter("rejected");
            reasons = new HashMap<>();
            for (String code : new String[]{"NULL_VALUE", "INVALID_UTF8", "INVALID_JSON", "INVALID_STRUCTURE",
                    "MISSING_CREATED_DATE", "INVALID_CREATED_DATE", "FUTURE_CREATED_DATE"}) {
                reasons.put(code, metrics.counter(code.toLowerCase(java.util.Locale.ROOT)));
            }
        }

        @Override
        public void processElement(RawHeartbeat record, Context ctx, Collector<HeartbeatMessage> out) {
            diagnostics.add("input", 1);
            diagnostics.observedOffset(record.topic, record.partition, record.offset);
            long now = System.currentTimeMillis();
            if (record.kafkaTimestampMs > 0) diagnostics.sampleMs("kafka_age", now - record.kafkaTimestampMs);
            long started = diagnostics.begin("validate");
            HeartbeatValidator.Result result;
            try { result = validator.validate(record, now); }
            finally { diagnostics.end("validate", started); }
            if (result.valid != null) {
                accepted.inc();
                diagnostics.add("accepted", 1);
                diagnostics.sampleMs("created_age", now - result.valid.createdTimeMs);
                started = diagnostics.begin("emit");
                try { out.collect(result.valid); }
                finally { diagnostics.end("emit", started); }
            } else {
                rejected.inc();
                reasons.get(result.errorCode).inc();
                diagnostics.add("rejected", 1);
                started = diagnostics.begin("emit");
                try { ctx.output(ERRORS, result.errorJson); }
                finally { diagnostics.end("emit", started); }
            }
        }

        @Override public void close() { if (diagnostics != null) diagnostics.close(); }
    }
}
