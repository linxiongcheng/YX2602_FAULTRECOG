package com.hitachi.yx2602.heartbeat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/** Thin-client settings. Credentials must never be included in logs. */
public final class IgniteConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final Set<String> KEYS = new HashSet<>(Arrays.asList(
            "ignite.addresses", "ignite.username", "ignite.password", "ignite.ssl.enabled",
            "ignite.request.timeout.ms", "ignite.transaction.timeout.ms", "ignite.retry.attempts",
            "ignite.retry.delay.ms", "ignite.batch.size", "ignite.queue.capacity",
            "ignite.batch.linger.ms", "ignite.flush.timeout.ms"));
    public final String addresses, username, password;
    public final boolean sslEnabled;
    public final int requestTimeoutMs, transactionTimeoutMs, attempts, batchSize, queueCapacity;
    public final long retryDelayMs, lingerMs, flushTimeoutMs;

    IgniteConfig(Properties p, long checkpointTimeoutMs) {
        addresses = p.getProperty("ignite.addresses", "").trim();
        username = p.getProperty("ignite.username", "").trim();
        password = p.getProperty("ignite.password", "");
        if (username.isEmpty() != password.isEmpty()) {
            throw new IllegalArgumentException("ignite.username和ignite.password必须同时配置");
        }
        String ssl = p.getProperty("ignite.ssl.enabled", "false").trim();
        if (!ssl.equals("true") && !ssl.equals("false")) {
            throw new IllegalArgumentException("ignite.ssl.enabled必须为true或false");
        }
        sslEnabled = Boolean.parseBoolean(ssl);
        requestTimeoutMs = integer(p, "ignite.request.timeout.ms", 10000);
        transactionTimeoutMs = integer(p, "ignite.transaction.timeout.ms", 10000);
        attempts = integer(p, "ignite.retry.attempts", 3);
        retryDelayMs = integer(p, "ignite.retry.delay.ms", 1000);
        batchSize = integer(p, "ignite.batch.size", 128);
        queueCapacity = integer(p, "ignite.queue.capacity", 1024);
        lingerMs = integer(p, "ignite.batch.linger.ms", 200);
        flushTimeoutMs = integer(p, "ignite.flush.timeout.ms", 60000);
        if (queueCapacity < batchSize || flushTimeoutMs >= checkpointTimeoutMs
                || flushTimeoutMs <= transactionTimeoutMs || flushTimeoutMs <= lingerMs) {
            throw new IllegalArgumentException("Ignite队列须不小于批大小；flush超时须大于事务超时和批等待时间、小于检查点超时");
        }
        if (!addresses.isEmpty()) validateAddresses();
    }

    public void requireAddresses() {
        if (addresses.isEmpty()) throw new IllegalArgumentException("--run/--check-ignite必须配置ignite.addresses");
    }

    private void validateAddresses() {
        for (String address : addresses.split(",", -1)) {
            address = address.trim();
            if (!address.matches("(?:[A-Za-z0-9_.-]+|\\[[0-9A-Fa-f:]+]):[0-9]{1,5}")) {
                throw new IllegalArgumentException("ignite.addresses须为逗号分隔的host:port");
            }
            int port = Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
            if (port < 1 || port > 65535) throw new IllegalArgumentException("Ignite端口超出范围");
        }
    }

    private static int integer(Properties p, String key, int fallback) {
        try {
            int value = Integer.parseInt(p.getProperty(key, Integer.toString(fallback)).trim());
            if (value > 0) return value;
        } catch (NumberFormatException ignored) { }
        throw new IllegalArgumentException(key + "必须为正整数且不超过2147483647");
    }
}
