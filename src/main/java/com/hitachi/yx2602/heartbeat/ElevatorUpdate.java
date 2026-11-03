package com.hitachi.yx2602.heartbeat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** 单梯状态及来源信息：拆分后经新旧判断，再用于更新最新状态和历史记录。 */
public class ElevatorUpdate {
    private static final ObjectMapper JSON = new ObjectMapper();
    public long elevatorId;
    public int port;
    /** 唯一业务时间，来自 CreatedDate；SendTime 仅作为附加信息保存。 */
    public long createdTimeMs;
    public Long sendTimeMs;
    public String sendTimeRaw;
    /** Flink 按电梯接受数据的序号，入库为 FLINK_SEQUENCE；历史槽位使用独立的 DB_SEQUENCE。 */
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
        update.sendTimeRaw = message.sendTimeRaw;
        update.createdTimeMs = message.createdTimeMs;
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
