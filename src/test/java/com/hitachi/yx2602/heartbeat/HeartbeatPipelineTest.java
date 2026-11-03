package com.hitachi.yx2602.heartbeat;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatPipelineTest {
    @Test void kafkaDeserializerRetainsMalformedBytesAndCoordinates() throws Exception {
        List<RawHeartbeat> output = new ArrayList<>();
        byte[] bytes = {(byte) 0xff};
        new HeartbeatReader.Deserializer().deserialize(new ConsumerRecord<>("test", 2, 14, null, bytes),
                new Collector<RawHeartbeat>() {
                    public void collect(RawHeartbeat value) { output.add(value); }
                    public void close() { }
                });
        assertEquals(1, output.size());
        assertArrayEquals(bytes, output.get(0).value);
        assertEquals(14, output.get(0).offset);
        assertEquals(2, output.get(0).partition);
    }

    @Test void graphRequiresCheckpointsAndIncludesBothBranchesWithStableUids() {
        ReaderConfig config = ReaderConfig.from(ReaderConfigTest.properties(), key -> null);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThrows(IllegalStateException.class, () -> HeartbeatReader.build(env, config));
        HeartbeatReader.configureEnvironment(env, config);
        HeartbeatReader.createGraph(env, config).print().uid("test-valid-output");
        List<String> uids = env.getStreamGraph().getStreamNodes().stream()
                .map(node -> node.getTransformationUID()).collect(Collectors.toList());
        assertTrue(uids.contains("heartbeat-kafka-source-v1"));
        assertTrue(uids.contains("heartbeat-validation-v1"));
        assertTrue(uids.contains("heartbeat-error-kafka-v1"));
        assertEquals(0, env.getCheckpointConfig().getTolerableCheckpointFailureNumber());
        assertTrue(TypeInformation.of(RawHeartbeat.class) instanceof PojoTypeInfo);
        assertTrue(TypeInformation.of(HeartbeatMessage.class) instanceof PojoTypeInfo);
    }

    @Test @Timeout(90)
    void actualFlinkRuntimeRoutesHistoricalValidAndInvalidMessages() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<HeartbeatMessage> valid = env.fromCollection(Arrays.asList(
                new RawHeartbeat("{\"CreatedDate\":\"1970/1/1 8:00:00.001\",\"SendTime\":\"\",\"EId1\":7}".getBytes(StandardCharsets.UTF_8),
                        null, "test", 0, 1, 1),
                new RawHeartbeat("{\"SendTime\":\"\"}".getBytes(StandardCharsets.UTF_8),
                        null, "test", 0, 2, 2),
                new RawHeartbeat(new byte[]{(byte) 0xff}, null, "test", 0, 3, 3)
        )).process(new HeartbeatReader.ValidationFunction(60000));
        DataStream<String> all = valid.map(value -> "valid:" + value.createdTimeMs)
                .union(valid.getSideOutput(HeartbeatReader.ValidationFunction.ERRORS));
        List<String> results = new ArrayList<>();
        try (CloseableIterator<String> iterator = all.executeAndCollect()) {
            iterator.forEachRemaining(results::add);
        }
        assertEquals(3, results.size());
        assertTrue(results.contains("valid:1"));
        assertEquals(1, results.stream().filter(value -> value.contains("MISSING_CREATED_DATE")).count());
        assertEquals(1, results.stream().filter(value -> value.contains("INVALID_UTF8")).count());
    }
}
