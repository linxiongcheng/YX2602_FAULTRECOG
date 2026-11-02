package com.hitachi.yx2602.heartbeat;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Expired heartbeats are dropped before field parsing; bad ports use a dedicated side output. */
public final class SplitHeartbeat extends ProcessFunction<HeartbeatMessage, ElevatorUpdate> {
    private static final long serialVersionUID = 1L;
    public static final OutputTag<String> ERRORS = new OutputTag<String>("heartbeat-parse-errors-v1") { };
    private final long maxAgeMs;
    private transient HeartbeatSplitter splitter;
    private transient Counter expired, emptyPorts, absentPorts, rejected, parsed;

    public SplitHeartbeat(long maxAgeMs) { this.maxAgeMs = maxAgeMs; }

    @Override
    public void open(OpenContext context) {
        splitter = new HeartbeatSplitter();
        MetricGroup metrics = getRuntimeContext().getMetricGroup().addGroup("heartbeat_parser");
        expired = metrics.counter("expired_heartbeats");
        emptyPorts = metrics.counter("empty_ports");
        absentPorts = metrics.counter("absent_ports");
        rejected = metrics.counter("rejected_records");
        parsed = metrics.counter("parsed_elevators");
    }

    @Override
    public void processElement(HeartbeatMessage message, Context context, Collector<ElevatorUpdate> out) {
        long now = context.timerService().currentProcessingTime();
        if (HeartbeatSplitter.expired(message.sendTimeMs, now, maxAgeMs)) {
            expired.inc();
            return;
        }
        HeartbeatSplitter.Result result = splitter.split(message, now);
        emptyPorts.inc(result.emptyPorts);
        absentPorts.inc(result.absentPorts);
        for (String error : result.errors) {
            rejected.inc();
            context.output(ERRORS, error);
        }
        for (ElevatorUpdate update : result.valid) {
            parsed.inc();
            out.collect(update);
        }
    }
}
