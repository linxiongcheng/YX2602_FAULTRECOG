package com.hitachi.yx2602.heartbeat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ParserPipelineTest {
    @Test @Timeout(90)
    void realFlinkRuntimeSplitsRoutesAndOrders() throws Exception {
        long now = System.currentTimeMillis();
        Properties properties = ReaderConfigTest.properties();
        properties.setProperty("parallelism", "1");
        ReaderConfig config = ReaderConfig.from(properties, key -> null);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        HeartbeatParser.Streams streams = HeartbeatParser.transform(env.fromCollection(Arrays.asList(
                HeartbeatSplitterTest.message(now - 3000,
                        "\"EId1\":11,\"EId2\":22,\"Speed2\":\"bad\",\"EId3\":33"),
                HeartbeatSplitterTest.message(now - 3000, "\"EId1\":11"),
                HeartbeatSplitterTest.message(now - 4000, "\"EId1\":11"),
                HeartbeatSplitterTest.message(now - 1000, "\"EId1\":11,\"Speed1\":\"98\""),
                HeartbeatSplitterTest.message(now - 700000, "\"EId1\":44")
        )), config);
        DataStream<String> all = streams.valid.map(ElevatorUpdate::toString).union(streams.errors);
        List<JsonNode> results = new ArrayList<>();
        ObjectMapper json = new ObjectMapper();
        try (CloseableIterator<String> iterator = all.executeAndCollect()) {
            while (iterator.hasNext()) results.add(json.readTree(iterator.next()));
        }
        assertEquals(4, results.size());
        assertEquals(1, results.stream().filter(node -> node.has("error_code")).count());
        List<JsonNode> elevator11 = results.stream().filter(node -> node.path("elevatorId").asLong() == 11)
                .collect(Collectors.toList());
        assertEquals(2, elevator11.size());
        assertEquals(1, elevator11.get(0).get("historySequence").asLong());
        assertEquals(2, elevator11.get(1).get("historySequence").asLong());
        assertEquals(98, elevator11.get(1).path("state").get("speed").asLong());
        assertEquals(1, results.stream().filter(node -> node.path("elevatorId").asLong() == 33).count());
    }

    @Test void productionGraphPreservesReaderUidsAndAddsParserAndErrorSink() {
        ReaderConfig config = ReaderConfig.from(ReaderConfigTest.properties(), key -> null);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        HeartbeatReader.configureEnvironment(env, config);
        HeartbeatParser.build(HeartbeatReader.createGraph(env, config), config)
                .print("heartbeat-parsed-preview").uid("heartbeat-parsed-preview-v1");
        Set<String> uids = env.getStreamGraph().getStreamNodes().stream()
                .map(node -> node.getTransformationUID()).collect(Collectors.toSet());
        assertTrue(uids.containsAll(Arrays.asList("heartbeat-kafka-source-v1", "heartbeat-validation-v1",
                "heartbeat-error-kafka-v1", "heartbeat-split-v1", "heartbeat-elevator-ordering-v1",
                "heartbeat-parser-error-kafka-v1", "heartbeat-parsed-preview-v1")));
        assertTrue(TypeInformation.of(ElevatorState.class) instanceof PojoTypeInfo);
        assertTrue(TypeInformation.of(ElevatorUpdate.class) instanceof PojoTypeInfo);
        assertTrue(TypeInformation.of(CreatedVersion.class) instanceof PojoTypeInfo);
    }

    @Test @Timeout(90) void readerAndParserUseCreatedDateForAgeOrderAndMetadata() throws Exception {
        long now = System.currentTimeMillis();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<RawHeartbeat> input = new ArrayList<>();
        String[] payloads = {
                TimeTestData.payload(now - 3000, "\"EId1\":11,\"SendTime\":9223372036854775807"),
                TimeTestData.payload(now - 2000, "\"EId1\":11,\"SendTime\":\"\""),
                TimeTestData.payload(now - 2500, "\"EId1\":11,\"SendTime\":9223372036854775807"),
                TimeTestData.payload(now - 2000, "\"EId1\":11,\"SendTime\":1"),
                TimeTestData.payload(now - 700000, "\"EId1\":22,\"SendTime\":" + now),
                "{\"EId1\":33,\"SendTime\":" + now + "}"
        };
        for (int i = 0; i < payloads.length; i++) input.add(new RawHeartbeat(
                payloads[i].getBytes(java.nio.charset.StandardCharsets.UTF_8), null, "test", 0, i, now));
        org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<HeartbeatMessage> read =
                env.fromCollection(input).process(new HeartbeatReader.ValidationFunction(60000));
        Properties properties = ReaderConfigTest.properties(); properties.setProperty("parallelism", "1");
        HeartbeatParser.Streams streams = HeartbeatParser.transform(read, ReaderConfig.from(properties, key -> null));
        DataStream<String> all = streams.valid.map(ElevatorUpdate::toString).union(streams.errors,
                read.getSideOutput(HeartbeatReader.ValidationFunction.ERRORS));
        List<JsonNode> results = new ArrayList<>();
        try (CloseableIterator<String> iterator = all.executeAndCollect()) {
            ObjectMapper mapper = new ObjectMapper();
            while (iterator.hasNext()) results.add(mapper.readTree(iterator.next()));
        }
        assertEquals(3, results.size());
        assertEquals(1, results.stream().filter(node -> node.path("error_code").asText().equals("MISSING_CREATED_DATE")).count());
        List<JsonNode> valid = results.stream().filter(node -> node.has("elevatorId")).collect(Collectors.toList());
        assertEquals(now - 3000, valid.get(0).get("createdTimeMs").asLong());
        assertEquals(now - 2000, valid.get(1).get("createdTimeMs").asLong());
        assertEquals(2, valid.get(1).get("historySequence").asLong());
        assertTrue(valid.get(1).get("sendTimeMs").isNull());
        assertEquals("\"\"", valid.get(1).get("sendTimeRaw").asText());
    }
}
