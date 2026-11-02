package com.hitachi.yx2602.heartbeat;

/** Flink POJO: retain bytes even when UTF-8 or JSON is malformed. */
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
