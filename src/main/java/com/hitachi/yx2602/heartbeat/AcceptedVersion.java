package com.hitachi.yx2602.heartbeat;

/** One checkpointed record per EId; no processing-time TTL. */
public class AcceptedVersion {
    public long sendTimeMs;
    public long historySequence;

    public AcceptedVersion() { }
    public AcceptedVersion(long sendTimeMs, long historySequence) {
        this.sendTimeMs = sendTimeMs;
        this.historySequence = historySequence;
    }
}
