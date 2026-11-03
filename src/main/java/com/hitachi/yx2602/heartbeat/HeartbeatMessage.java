package com.hitachi.yx2602.heartbeat;

/** 通过整条校验、尚未拆分的心跳；payloadJson 保留上游全部字段。 */
public class HeartbeatMessage {
    public String payloadJson;
    public String sourceTopic;
    public int sourcePartition;
    public long sourceOffset;
    public long kafkaTimestampMs;
    public long receivedAtMs;
    /** CreatedDate 转换的业务时间，供过期过滤、去重和入库比较使用。 */
    public long createdTimeMs;
    public Long sendTimeMs;
    /** SendTime 的 JSON 表示；字符串带 JSON 引号，字段缺失时为 null。 */
    public String sendTimeRaw;

    public HeartbeatMessage() { }

    public HeartbeatMessage(String payloadJson, RawHeartbeat source, long receivedAtMs, long createdTimeMs) {
        this.payloadJson = payloadJson;
        this.sourceTopic = source.topic;
        this.sourcePartition = source.partition;
        this.sourceOffset = source.offset;
        this.kafkaTimestampMs = source.kafkaTimestampMs;
        this.receivedAtMs = receivedAtMs;
        this.createdTimeMs = createdTimeMs;
    }

    @Override
    public String toString() {
        // 此对象的摘要只展示来源和时间；单梯预览 JSON 由 ElevatorUpdate 输出。
        return "HeartbeatMessage{" + sourceTopic + ":" + sourcePartition + ":" + sourceOffset
                + ", createdTimeMs=" + createdTimeMs + ", receivedAtMs=" + receivedAtMs + "}";
    }
}
