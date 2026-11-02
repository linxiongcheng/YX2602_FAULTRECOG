package com.hitachi.yx2602.heartbeat;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public final class ValidateHeartbeat extends ProcessFunction<RawHeartbeat, HeartbeatMessage> {
    private static final long serialVersionUID = 1L;
    public static final OutputTag<String> ERRORS = new OutputTag<String>("heartbeat-errors-v1") { };
    private final long futureSkewMs;
    private transient HeartbeatValidator validator;
    private transient Counter accepted, rejected;
    private transient Map<String, Counter> reasons;

    public ValidateHeartbeat(long futureSkewMs) { this.futureSkewMs = futureSkewMs; }

    @Override
    public void open(OpenContext context) {
        validator = new HeartbeatValidator(futureSkewMs);
        MetricGroup metrics = getRuntimeContext().getMetricGroup().addGroup("heartbeat_reader");
        accepted = metrics.counter("accepted");
        rejected = metrics.counter("rejected");
        reasons = new HashMap<>();
        for (String code : new String[]{"NULL_VALUE", "INVALID_UTF8", "INVALID_JSON", "INVALID_STRUCTURE",
                "MISSING_SEND_TIME", "INVALID_SEND_TIME", "FUTURE_SEND_TIME"}) {
            reasons.put(code, metrics.counter(code.toLowerCase(java.util.Locale.ROOT)));
        }
    }

    @Override
    public void processElement(RawHeartbeat record, Context ctx, Collector<HeartbeatMessage> out) {
        HeartbeatValidator.Result result = validator.validate(record, System.currentTimeMillis());
        if (result.valid != null) {
            accepted.inc();
            out.collect(result.valid);
        } else {
            rejected.inc();
            reasons.get(result.errorCode).inc();
            ctx.output(ERRORS, result.errorJson);
        }
    }
}
