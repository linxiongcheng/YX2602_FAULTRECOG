package com.hitachi.yx2602.heartbeat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** One accepted snapshot supplies both the future latest-state write and history write. */
public class ElevatorUpdate {
    private static final ObjectMapper JSON = new ObjectMapper();
    public long elevatorId;
    public int port;
    public long sendTimeMs;
    public long historySequence;
    public long receivedAtMs;
    public long parsedAtMs;
    public String sourceTopic;
    public int sourcePartition;
    public long sourceOffset;
    public long kafkaTimestampMs;
    public ElevatorState state;

    public ElevatorUpdate() { }

    static ElevatorUpdate from(HeartbeatMessage message, int port, long id, ElevatorState state) {
        ElevatorUpdate update = new ElevatorUpdate();
        update.elevatorId = id;
        update.port = port;
        update.sendTimeMs = message.sendTimeMs;
        update.receivedAtMs = message.receivedAtMs;
        update.sourceTopic = message.sourceTopic;
        update.sourcePartition = message.sourcePartition;
        update.sourceOffset = message.sourceOffset;
        update.kafkaTimestampMs = message.kafkaTimestampMs;
        update.state = state;
        return update;
    }

    @Override
    public String toString() {
        try { return JSON.writeValueAsString(this); }
        catch (JsonProcessingException e) { throw new IllegalStateException("无法序列化单梯状态", e); }
    }
}
