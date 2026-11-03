package com.hitachi.yx2602.heartbeat;

/** Kafka 原始消息：保留字节和来源位点，便于追溯非法 UTF-8 或 JSON；无参构造供 Flink 序列化使用。 */
public class RawHeartbeat {
    public byte[] value;
    public byte[] key;
    public String topic;
    public int partition;
    public long offset;
    public long kafkaTimestampMs;

    public RawHeartbeat() { }

    public RawHeartbeat(byte[] value, byte[] key, String topic, int partition, long offset, long timestamp) {
        this.value = value;
        this.key = key;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.kafkaTimestampMs = timestamp;
    }
}
