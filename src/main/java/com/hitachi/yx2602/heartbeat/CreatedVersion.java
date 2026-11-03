package com.hitachi.yx2602.heartbeat;

/** 每台电梯最近接受的业务时间和 Flink 序号，随检查点保存；不按处理时间设置过期。 */
public class CreatedVersion {
    public long createdTimeMs;
    public long historySequence;

    public CreatedVersion() { }
    public CreatedVersion(long createdTimeMs, long historySequence) {
        this.createdTimeMs = createdTimeMs;
        this.historySequence = historySequence;
    }
}
