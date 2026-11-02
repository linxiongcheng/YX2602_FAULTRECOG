package com.hitachi.yx2602.heartbeat;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Pure validation, deliberately independent of event age, elevator split and deduplication. */
public final class HeartbeatValidator {
    private final ObjectMapper mapper = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(100).build()).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private final long futureSkewMs;

    public HeartbeatValidator(long futureSkewMs) {
        if (futureSkewMs < 0) throw new IllegalArgumentException("时钟偏差不能为负");
        this.futureSkewMs = futureSkewMs;
    }

    public Result validate(RawHeartbeat record, long nowMs) {
        if (record.value == null) return reject(record, nowMs, "NULL_VALUE", "Kafka消息值为空（tombstone）");
        String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(record.value)).toString();
        } catch (CharacterCodingException e) {
            return reject(record, nowMs, "INVALID_UTF8", "消息不是合法UTF-8");
        }
        JsonNode payload;
        try { payload = mapper.readTree(text); }
        catch (IOException e) { return reject(record, nowMs, "INVALID_JSON", "JSON非法、重复键或超过解析限制"); }
        if (payload == null || !payload.isObject()) {
            return reject(record, nowMs, "INVALID_STRUCTURE", "JSON根节点必须是对象");
        }
        JsonNode sent = payload.get("SendTime");
        if (sent == null || sent.isNull() || (sent.isTextual() && sent.textValue().isEmpty())) {
            return reject(record, nowMs, "MISSING_SEND_TIME", "缺少SendTime或值为空");
        }
        if (!sent.isIntegralNumber() || !sent.canConvertToLong() || sent.longValue() <= 0) {
            return reject(record, nowMs, "INVALID_SEND_TIME", "SendTime必须是正的64位整数毫秒时间戳");
        }
        long sendTime = sent.longValue();
        // Subtraction is safe for positive epoch values; avoid overflowing now + allowed skew.
        if (sendTime > nowMs && sendTime - nowMs > futureSkewMs) {
            return reject(record, nowMs, "FUTURE_SEND_TIME", "SendTime超过允许的时钟超前阈值");
        }
        return new Result(new HeartbeatMessage(text, record, nowMs, sendTime), null, null);
    }

    private Result reject(RawHeartbeat record, long nowMs, String code, String reason) {
        ObjectNode error = mapper.createObjectNode();
        error.put("schema_version", 1);
        error.put("message_id", record.topic + ":" + record.partition + ":" + record.offset);
        ObjectNode source = error.putObject("source");
        source.put("topic", record.topic);
        source.put("partition", record.partition);
        source.put("offset", record.offset);
        source.put("kafka_timestamp_ms", record.kafkaTimestampMs);
        error.put("processed_at_ms", nowMs);
        error.put("error_code", code);
        error.put("error_reason", reason);
        putBytes(error, "raw_value_base64", record.value);
        putBytes(error, "raw_key_base64", record.key);
        // No payload-dependent serialization: byte arrays have been safely encoded as Base64.
        return new Result(null, code, error.toString());
    }

    private static void putBytes(ObjectNode node, String name, byte[] value) {
        if (value == null) node.putNull(name);
        else node.put(name, Base64.getEncoder().encodeToString(value));
    }

    public static final class Result {
        public final HeartbeatMessage valid;
        public final String errorCode;
        public final String errorJson;

        private Result(HeartbeatMessage valid, String errorCode, String errorJson) {
            this.valid = valid;
            this.errorCode = errorCode;
            this.errorJson = errorJson;
        }
    }
}
