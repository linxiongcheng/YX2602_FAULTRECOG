package com.hitachi.yx2602.heartbeat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatValidatorTest {
    private static final long NOW = 1793318400000L;
    private final HeartbeatValidator validator = new HeartbeatValidator(60000);

    private RawHeartbeat raw(String json) {
        return new RawHeartbeat(json.getBytes(StandardCharsets.UTF_8), null, "heartbeat", 2, 42, NOW - 20);
    }

    @Test void preservesHistoricalAndUnknownFieldsWithoutSplitting() {
        String json = TimeTestData.payload(1, "\"SendTime\":\"\",\"EId1\":7,\"EId2\":0,\"new_field\":{\"a\":true}");
        HeartbeatValidator.Result result = validator.validate(raw(json), NOW);
        assertNull(result.errorJson);
        assertEquals(json, result.valid.payloadJson);
        assertEquals(1, result.valid.createdTimeMs);
        assertNull(result.valid.sendTimeMs);
        assertEquals("\"\"", result.valid.sendTimeRaw);
        assertEquals(42, result.valid.sourceOffset);
        assertEquals(NOW, result.valid.receivedAtMs);
        assertEquals(NOW - 20, result.valid.kafkaTimestampMs);
    }

    @Test void allowsExactlySixtySecondsButRejectsOneMillisecondMore() {
        assertNotNull(validator.validate(raw(TimeTestData.payload(NOW + 60000, "")), NOW).valid);
        assertEquals("FUTURE_CREATED_DATE", validator.validate(
                raw(TimeTestData.payload(NOW + 60001, "")), NOW).errorCode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"CreatedDate\":null}", "{\"CreatedDate\":\"\"}", "{\"CreatedDate\":\"  \"}"})
    void missingCreatedDateGoesToErrorsEvenWhenSendTimeExists(String json) {
        assertEquals("MISSING_CREATED_DATE", validator.validate(raw(json), NOW).errorCode);
        assertEquals("MISSING_CREATED_DATE", validator.validate(raw("{\"SendTime\":" + NOW + "}"), NOW).errorCode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false", "0", "1.0", "\"1793318400000\"", "[]", "{}",
            "\"2026/2/30 12:00:00\"", "\"2026/1/1 24:00:00\"", "\"2026/1/1 12:00:60\"",
            "\"2026/1/1 12:00:00.1234\"", "\"2026/1/1\"", "\"2026-01-01T12:00:00Z\""})
    void invalidCreatedDateTypesAndDates(String value) {
        assertEquals("INVALID_CREATED_DATE", validator.validate(raw("{\"CreatedDate\":" + value + "}"), NOW).errorCode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "\"\"", "true", "false", "0", "-1", "1.5", "1e3", "\"bad\"",
            "9223372036854775808", "[]", "{}", "9223372036854775807"})
    void sendTimeNeverControlsEventValidity(String value) throws Exception {
        HeartbeatValidator.Result result = validator.validate(raw(TimeTestData.payload(NOW, "\"SendTime\":" + value)), NOW);
        assertNull(result.errorCode);
        assertEquals(NOW, result.valid.createdTimeMs);
        assertEquals(new ObjectMapper().readTree(value), new ObjectMapper().readTree(result.valid.sendTimeRaw));
    }

    @Test void sourceTimeIsOptionalAndDoesNotReplaceBusinessTime() {
        HeartbeatMessage noSourceTime = validator.validate(raw(TimeTestData.payload(NOW, "")), NOW).valid;
        assertNotNull(noSourceTime);
        assertNull(noSourceTime.sendTimeMs);
        assertNull(noSourceTime.sendTimeRaw);
        HeartbeatMessage withSourceTime = validator.validate(raw(TimeTestData.payload(NOW, "\"SendTime\":1")), NOW).valid;
        assertEquals(1L, withSourceTime.sendTimeMs);
        assertEquals(NOW, withSourceTime.createdTimeMs);
    }

    @Test void parsesDocumentExampleInBeijingRegardlessOfJvmTimezone() {
        java.util.TimeZone original = java.util.TimeZone.getDefault();
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/New_York"));
            long expected = java.time.Instant.parse("2026-01-22T09:56:17Z").toEpochMilli();
            assertEquals(expected, HeartbeatTime.parseCreatedDate("2026/1/22 17:56:17"));
            assertEquals(expected, HeartbeatTime.parseCreatedDate("2026/01/22 17:56:17"));
            assertEquals(expected + 100, HeartbeatTime.parseCreatedDate("2026/1/22 17:56:17.1"));
            assertEquals(expected + 123, HeartbeatTime.parseCreatedDate(" 2026/1/22 17:56:17.123 "));
        } finally { java.util.TimeZone.setDefault(original); }
    }

    @ParameterizedTest
    @ValueSource(strings = {"{", "{\"SendTime\":1,\"SendTime\":2}",
            "{\"SendTime\":1} {}", "{\"SendTime\":NaN}", "{\"SendTime\":Infinity}",
            "{\"SendTime\":1,}", "{\"SendTime\":1,\"nested\":{\"a\":1,\"a\":2}}"})
    void malformedAndAmbiguousJson(String json) {
        assertEquals("INVALID_JSON", validator.validate(raw(json), NOW).errorCode);
    }

    @ParameterizedTest
    @ValueSource(strings = {"[]", "null", "123", "\"text\"", ""})
    void requiresObjectRoot(String json) {
        assertEquals("INVALID_STRUCTURE", validator.validate(raw(json), NOW).errorCode);
    }

    @Test void malformedUtf8IsPreservedByteForByteWithKafkaCoordinates() throws Exception {
        byte[] bytes = {(byte) 0xc3, (byte) 0x28, 0};
        RawHeartbeat record = new RawHeartbeat(bytes, new byte[]{0, (byte) 0xff}, "raw", 3, 99, NOW);
        HeartbeatValidator.Result result = validator.validate(record, NOW);
        JsonNode error = new ObjectMapper().readTree(result.errorJson);
        assertEquals("INVALID_UTF8", result.errorCode);
        assertArrayEquals(bytes, Base64.getDecoder().decode(error.get("raw_value_base64").asText()));
        assertArrayEquals(record.key, Base64.getDecoder().decode(error.get("raw_key_base64").asText()));
        assertEquals("raw:3:99", error.get("message_id").asText());
        assertEquals(99, error.path("source").path("offset").asLong());
    }

    @Test void nullAndEmptyValuesRemainDistinctInErrorEnvelope() throws Exception {
        RawHeartbeat record = new RawHeartbeat(null, null, "raw", 0, 1, -1);
        JsonNode error = new ObjectMapper().readTree(validator.validate(record, NOW).errorJson);
        assertTrue(error.get("raw_value_base64").isNull());
        assertEquals("NULL_VALUE", error.get("error_code").asText());
        assertEquals("", new ObjectMapper().readTree(validator.validate(raw(""), NOW).errorJson)
                .get("raw_value_base64").asText());
    }

    @Test void futureThresholdDoesNotOverflow() {
        HeartbeatValidator permissive = new HeartbeatValidator(Long.MAX_VALUE);
        assertNotNull(permissive.validate(raw("{\"CreatedDate\":\"9999/12/31 23:59:59.999\"}"), NOW).valid);
    }

    @Test void deepJsonIsQuarantinedInsteadOfCrashingTask() {
        StringBuilder text = new StringBuilder("{\"SendTime\":1,\"nested\":");
        for (int i = 0; i < 110; i++) text.append('[');
        text.append('0');
        for (int i = 0; i < 110; i++) text.append(']');
        text.append('}');
        assertEquals("INVALID_JSON", validator.validate(raw(text.toString()), NOW).errorCode);
    }
}
