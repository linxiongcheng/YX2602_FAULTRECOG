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
        assertTrue(TypeInformation.of(AcceptedVersion.class) instanceof PojoTypeInfo);
    }
}
