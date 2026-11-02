package com.hitachi.yx2602.heartbeat;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class AcceptNewerState extends KeyedProcessFunction<Long, ElevatorUpdate, ElevatorUpdate> {
    private static final long serialVersionUID = 1L;
    private final long maxAgeMs;
    private transient ValueState<AcceptedVersion> version;
    private transient Counter accepted, duplicate, older, expired;

    public AcceptNewerState(long maxAgeMs) { this.maxAgeMs = maxAgeMs; }

    @Override
    public void open(OpenContext context) {
        version = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "last-accepted-elevator-version-v1", AcceptedVersion.class));
        MetricGroup metrics = getRuntimeContext().getMetricGroup().addGroup("heartbeat_parser");
        accepted = metrics.counter("accepted_elevators");
        duplicate = metrics.counter("duplicate_elevators");
        older = metrics.counter("older_elevators");
        expired = metrics.counter("expired_elevators");
    }

    @Override
    public void processElement(ElevatorUpdate update, Context context, Collector<ElevatorUpdate> out)
            throws Exception {
        long now = context.timerService().currentProcessingTime();
        // Check again after keyBy: shuffle/backpressure may have consumed the remaining freshness.
        if (HeartbeatSplitter.expired(update.sendTimeMs, now, maxAgeMs)) {
            expired.inc();
            return;
        }
        AcceptedVersion previous = version.value();
        if (previous != null && update.sendTimeMs <= previous.sendTimeMs) {
            if (update.sendTimeMs == previous.sendTimeMs) duplicate.inc();
            else older.inc();
            return;
        }
        long nextSequence = previous == null ? 1 : Math.incrementExact(previous.historySequence);
        update.historySequence = nextSequence;
        update.parsedAtMs = now;
        version.update(new AcceptedVersion(update.sendTimeMs, nextSequence));
        accepted.inc();
        out.collect(update);
    }
}
