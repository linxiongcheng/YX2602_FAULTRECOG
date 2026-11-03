package com.hitachi.yx2602.heartbeat;

import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable copy at the async boundary, including when Flink object reuse is enabled. */
final class HeartbeatRow {
    final long elevatorId;
    final long sendTimeMs;
    final Map<String, Object> values;

    private HeartbeatRow(ElevatorUpdate update) {
        if (update.elevatorId <= 0 || update.sendTimeMs <= 0 || update.state == null) {
            throw new IllegalArgumentException("写入必须包含有效电梯ID、发送时间和状态");
        }
        elevatorId = update.elevatorId;
        sendTimeMs = update.sendTimeMs;
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("PORT", update.port);
        fields.put("SEND_TIME_MS", update.sendTimeMs);
        fields.put("FLINK_SEQUENCE", update.historySequence);
        fields.put("RECEIVED_AT_MS", update.receivedAtMs);
        fields.put("PARSED_AT_MS", update.parsedAtMs);
        fields.put("SOURCE_TOPIC", update.sourceTopic);
        fields.put("SOURCE_PARTITION", update.sourcePartition);
        fields.put("SOURCE_OFFSET", update.sourceOffset);
        fields.put("KAFKA_TIMESTAMP_MS", update.kafkaTimestampMs);
        for (Map.Entry<String, java.lang.reflect.Field> entry : IgniteSchema.STATE_FIELDS.entrySet()) {
            try { fields.put(entry.getKey(), entry.getValue().get(update.state)); }
            catch (IllegalAccessException e) { throw new IllegalStateException(e); }
        }
        values = java.util.Collections.unmodifiableMap(fields);
    }

    static HeartbeatRow copyOf(ElevatorUpdate update) { return new HeartbeatRow(update); }
}
