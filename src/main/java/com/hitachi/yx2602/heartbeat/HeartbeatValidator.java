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

/** 整条消息校验：检查编码、JSON 和 CreatedDate；过期过滤、单梯拆分及去重由解析模块处理。 */
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
        JsonNode created = payload.get("CreatedDate");
        if (HeartbeatTime.missing(created)) {
            return reject(record, nowMs, "MISSING_CREATED_DATE", "缺少CreatedDate或值为空");
        }
        long createdTime;
        try { createdTime = HeartbeatTime.parseCreatedDate(created); }
        catch (java.time.DateTimeException | IllegalArgumentException e) {
            return reject(record, nowMs, "INVALID_CREATED_DATE", "CreatedDate不是有效的北京时间日期，要求yyyy/M/d H:mm:ss，可选1～3位毫秒小数");
        }
        // 用时间差判断超前幅度，避免“当前时间 + 容忍值”发生加法溢出。
        if (createdTime > nowMs && createdTime - nowMs > futureSkewMs) {
            return reject(record, nowMs, "FUTURE_CREATED_DATE", "CreatedDate超过允许的时钟超前阈值");
        }
        // SendTime 仅保留用于追溯，缺失或格式不合法不影响消息按 CreatedDate 继续处理。
        JsonNode sent = payload.get("SendTime");
        HeartbeatMessage message = new HeartbeatMessage(text, record, nowMs, createdTime);
        message.sendTimeMs = HeartbeatTime.optionalSendTime(sent);
        message.sendTimeRaw = sent == null ? null : sent.toString();
        return new Result(message, null, null);
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
        // 原始字节用 Base64 保存，非法 UTF-8 或 JSON 也不会破坏异常消息的序列化。
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
