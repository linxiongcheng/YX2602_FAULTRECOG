package com.hitachi.yx2602.heartbeat;

/** Validated but unsplit heartbeat. payloadJson preserves all upstream fields. */
public class HeartbeatMessage {
    public String payloadJson;
    public String sourceTopic;
    public int sourcePartition;
    public long sourceOffset;
    public long kafkaTimestampMs;
    public long receivedAtMs;
    public long sendTimeMs;

    public HeartbeatMessage() { }

    public HeartbeatMessage(String payloadJson, RawHeartbeat source, long receivedAtMs, long sendTimeMs) {
        this.payloadJson = payloadJson;
        this.sourceTopic = source.topic;
        this.sourcePartition = source.partition;
        this.sourceOffset = source.offset;
        this.kafkaTimestampMs = source.kafkaTimestampMs;
        this.receivedAtMs = receivedAtMs;
        this.sendTimeMs = sendTimeMs;
    }

    @Override
    public String toString() {
        // Preview deliberately avoids logging full elevator data.
        return "HeartbeatMessage{" + sourceTopic + ":" + sourcePartition + ":" + sourceOffset
                + ", sendTimeMs=" + sendTimeMs + ", receivedAtMs=" + receivedAtMs + "}";
    }
}
