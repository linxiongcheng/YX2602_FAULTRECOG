package com.hitachi.yx2602.heartbeat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatSplitterTest {
    static final long NOW = 1793318400000L;
    private final HeartbeatSplitter splitter = new HeartbeatSplitter();

    static HeartbeatMessage message(long time, String fields) {
        String created = fields.contains("\"CreatedDate\"") ? "" : "\"CreatedDate\":\"" + TimeTestData.format(time) + "\",";
        String json = "{" + created + "\"SendTime\":" + time + (fields.isEmpty() ? "" : "," + fields) + "}";
        HeartbeatMessage message = new HeartbeatMessage(json, new RawHeartbeat(json.getBytes(StandardCharsets.UTF_8),
                null, "heartbeat3", 2, 42, time), NOW, time);
        message.sendTimeMs = time; message.sendTimeRaw = Long.toString(time);
        return message;
    }

    @Test void badSecondPortDoesNotPreventFirstAndThirdOutputs() throws Exception {
        HeartbeatMessage input = message(NOW, "\"EId1\":11,\"EId2\":22,\"Speed2\":\"abc\",\"EId3\":33");
        HeartbeatSplitter.Result result = splitter.split(input, NOW);
        assertEquals(2, result.valid.size());
        assertEquals(11, result.valid.get(0).elevatorId);
        assertEquals(33, result.valid.get(1).elevatorId);
        assertEquals(3, result.valid.get(1).port);
        assertEquals(1, result.errors.size());
        JsonNode error = new ObjectMapper().readTree(result.errors.get(0));
        assertEquals("parsing", error.get("stage").asText());
        assertEquals(2, error.get("port").asInt());
        assertEquals(22, error.get("elevator_id").asLong());
        assertEquals("Speed2", error.get("error_field").asText());
        assertEquals(42, error.path("source").path("offset").asLong());
        assertEquals(input.payloadJson, new String(Base64.getDecoder().decode(
                error.get("raw_value_base64").asText()), StandardCharsets.UTF_8));
    }

    @Test void normalizesIntegersPreservesCodesNullsAndFullMetadata() {
        HeartbeatSplitter.Result result = splitter.split(message(NOW,
                "\"EId1\":\"0011\",\"Floor1\":\"-2\",\"Speed1\":\" 98 \",\"DoorLock1\":\"1\","
                + "\"RunTotal1\":4294967295,\"DoorCounter1\":\"12\",\"SecDoorCouter1\":7,"
                + "\"MainFaultCode1\":\"0007\",\"SubFaultCode1\":0,\"Code\":\"001234\","
                + "\"Weight1\":null,\"SignalRssi\":\"31\",\"NetworkType\":\"2\","
                + "\"EId2\":0,\"EId3\":0"), NOW);
        assertTrue(result.errors.isEmpty());
        assertEquals(1, result.valid.size());
        ElevatorUpdate update = result.valid.get(0);
        assertEquals(11, update.elevatorId);
        assertEquals(-2, update.state.floor.longValue());
        assertEquals(98, update.state.speed.longValue());
        assertEquals(1, update.state.doorLock.longValue());
        assertEquals(4294967295L, update.state.runTotal.longValue());
        assertEquals(12, update.state.doorCounter.longValue());
        assertEquals(7, update.state.secDoorCouter.longValue());
        assertEquals("0007", update.state.mainFaultCode);
        assertEquals("0", update.state.subFaultCode);
        assertEquals("001234", update.state.code);
        assertNull(update.state.weight);
        assertNull(update.state.isRepair);
        assertEquals(31, update.state.signalRssi.longValue());
        assertEquals(2, update.state.networkType.longValue());
        assertEquals(TimeTestData.format(NOW), update.state.createdDate);
        assertEquals(NOW, update.createdTimeMs);
        assertEquals("heartbeat3", update.sourceTopic);
        assertEquals(42, update.sourceOffset);
        assertEquals(NOW, update.sendTimeMs);
        assertEquals(0, update.historySequence); // Assigned only after keyed acceptance.
        assertEquals(2, result.emptyPorts);
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "1.5", "\"1.0\"", "\"1e2\"", "\"abc\"", "\"\"",
            "9223372036854775808", "\"9223372036854775808\"", "-1", "[]", "{}"})
    void invalidNumericValueRejectsOnlyItsElevator(String value) {
        HeartbeatSplitter.Result result = splitter.split(message(NOW,
                "\"EId1\":11,\"Speed1\":" + value + ",\"EId3\":33"), NOW);
        assertEquals(1, result.errors.size());
        assertEquals(1, result.valid.size());
        assertEquals(33, result.valid.get(0).elevatorId);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"DoorLock1\":2", "\"IsPower1\":-1", "\"TCDA1\":3",
            "\"Floor1\":2147483648", "\"SignalRssi\":32", "\"NetworkType\":3",
            "\"MainFaultCode1\":true", "\"SubFaultCode1\":1.5", "\"CreatedDate\":123"})
    void checksDocumentedRangesAndCodeTypes(String field) {
        HeartbeatSplitter.Result result = splitter.split(message(NOW, "\"EId1\":11," + field), NOW);
        assertEquals(1, result.errors.size());
        assertTrue(result.valid.isEmpty());
    }

    @Test void zeroIdSkipsGarbageFieldsAndAbsentPortsAreOptional() {
        HeartbeatSplitter.Result result = splitter.split(message(NOW,
                "\"EId1\":0,\"Speed1\":\"bad\",\"EId3\":33"), NOW);
        assertTrue(result.errors.isEmpty());
        assertEquals(1, result.emptyPorts);
        assertEquals(1, result.absentPorts);
        assertEquals(33, result.valid.get(0).elevatorId);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"EId1\":null", "\"EId1\":-1", "\"EId1\":\"x\"", "\"Floor1\":3"})
    void invalidOrMissingIdForPresentPortIsNotSilentlySkipped(String port) {
        HeartbeatSplitter.Result result = splitter.split(message(NOW, port + ",\"EId3\":33"), NOW);
        assertEquals(1, result.errors.size());
        assertTrue(result.errors.get(0).contains("INVALID_ELEVATOR_ID"));
        assertEquals(33, result.valid.get(0).elevatorId);
    }

    @Test void duplicatesWithinPacketAreAmbiguousButOtherElevatorSurvives() {
        HeartbeatSplitter.Result result = splitter.split(message(NOW,
                "\"EId1\":11,\"EId2\":\"11\",\"EId3\":33"), NOW);
        assertEquals(2, result.errors.size());
        assertTrue(result.errors.stream().allMatch(error -> error.contains("DUPLICATE_ELEVATOR_ID")));
        assertEquals(33, result.valid.get(0).elevatorId);
    }

    @Test void missingAllElevatorDataIsAnErrorButAllZeroPortsAreNot() {
        assertTrue(splitter.split(message(NOW, ""), NOW).errors.get(0).contains("NO_ELEVATOR_DATA"));
        HeartbeatSplitter.Result result = splitter.split(message(NOW, "\"EId1\":0,\"EId2\":0,\"EId3\":0"), NOW);
        assertTrue(result.errors.isEmpty());
        assertTrue(result.valid.isEmpty());
    }

    @Test void invalidSharedFieldRejectsEveryNonemptyPort() {
        HeartbeatSplitter.Result result = splitter.split(message(NOW,
                "\"EId1\":11,\"EId2\":0,\"EId3\":33,\"SignalRssi\":32"), NOW);
        assertEquals(2, result.errors.size());
        assertTrue(result.valid.isEmpty());
    }

    @Test void previewJsonIncludesUnknownFieldsAsNullAndCodesUnchanged() throws Exception {
        ElevatorUpdate update = splitter.split(message(NOW,
                "\"EId1\":11,\"MainFaultCode1\":\"0007\""), NOW).valid.get(0);
        JsonNode preview = new ObjectMapper().readTree(update.toString());
        assertTrue(preview.path("state").get("speed").isNull());
        assertEquals("0007", preview.path("state").get("mainFaultCode").asText());
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "\"\"", "\"  \""})
    void emptyUpdatedDateIsNull(String value) {
        HeartbeatSplitter.Result result = splitter.split(message(NOW, "\"EId1\":11,\"UpdatedDate\":" + value), NOW);
        assertTrue(result.errors.isEmpty());
        assertNull(result.valid.get(0).state.updatedDate);
    }

    @ParameterizedTest
    @ValueSource(strings = {"123", "\"bad\"", "\"2026/2/30 12:00:00\"", "\"2026/11/5 24:00:00\"", "\"2026/11/5 12:00:00.1234\""})
    void invalidUpdatedDateIsQuarantinedBeforeSink(String value) throws Exception {
        HeartbeatSplitter.Result result = splitter.split(message(NOW,
                "\"EId1\":11,\"EId3\":33,\"UpdatedDate\":" + value), NOW);
        assertTrue(result.valid.isEmpty());
        assertEquals(2, result.errors.size());
        JsonNode error = new ObjectMapper().readTree(result.errors.get(0));
        assertEquals("UpdatedDate", error.path("error_field").asText());
        assertEquals("INVALID_FIELD", error.path("error_code").asText());
    }

    @Test void validUpdatedDateDoesNotReplaceCreatedDateForOrdering() {
        String updated = "2026/1/22 0:00:00.123";
        HeartbeatSplitter.Result result = splitter.split(message(NOW,
                "\"EId1\":11,\"UpdatedDate\":\"" + updated + "\""), NOW);
        assertTrue(result.errors.isEmpty());
        assertEquals(NOW, result.valid.get(0).createdTimeMs);
        assertEquals(updated, result.valid.get(0).state.updatedDate);
    }
}
