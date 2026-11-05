package com.hitachi.yx2602.heartbeat;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 提交时加载并校验读取、解析和检查点配置，支持环境变量占位符；勿在日志中输出凭据。 */
public final class ReaderConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern ENV = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)}");
    public final String bootstrapServers, inputTopic, errorTopic, groupId, checkpointStorage, jobName;
    public final int parallelism, restartAttempts, deliveryTimeoutMs, maxRequestBytes;
    public final long futureSkewMs, checkpointIntervalMs, checkpointTimeoutMs, restartDelayMs;
    public final long parserMaxAgeMs;
    public final long monitorLogIntervalMs;
    public final int parserMaxParallelism;
    public final IgniteConfig ignite;
    private final Properties connectionProperties = new Properties();

    private ReaderConfig(Properties p) {
        Set<String> known = new HashSet<>(Arrays.asList(
                "kafka.bootstrap.servers", "kafka.input.topic", "kafka.error.topic", "kafka.group.id",
                "checkpoint.storage", "job.name", "parallelism", "validation.max.future.skew.ms",
                "checkpoint.interval.ms", "checkpoint.timeout.ms", "kafka.delivery.timeout.ms",
                "restart.attempts", "restart.delay.ms", "kafka.error.max.request.bytes",
                "parser.max.age.ms", "parser.max.parallelism", "monitor.log.interval.ms"));
        for (String key : p.stringPropertyNames()) {
            if (key.startsWith("kafka.client.")) {
                String clientKey = key.substring("kafka.client.".length());
                if (!(clientKey.startsWith("security.") || clientKey.startsWith("sasl.")
                        || clientKey.startsWith("ssl.") || clientKey.equals("client.dns.lookup"))) {
                    throw new IllegalArgumentException("禁止覆盖消费或可靠性设置: " + key);
                }
                connectionProperties.setProperty(clientKey, p.getProperty(key));
            } else if (!known.contains(key) && !IgniteConfig.KEYS.contains(key)) {
                throw new IllegalArgumentException("未知配置项: " + key);
            }
        }
        bootstrapServers = required(p, "kafka.bootstrap.servers");
        inputTopic = topic(required(p, "kafka.input.topic"));
        errorTopic = topic(required(p, "kafka.error.topic"));
        if (inputTopic.equals(errorTopic)) {
            throw new IllegalArgumentException("输入Topic和异常Topic不能相同");
        }
        groupId = required(p, "kafka.group.id");
        checkpointStorage = required(p, "checkpoint.storage");
        URI storage = URI.create(checkpointStorage);
        if (!Arrays.asList("hdfs", "file", "s3", "s3a").contains(storage.getScheme())
                || storage.getPath() == null || !storage.getPath().startsWith("/")) {
            throw new IllegalArgumentException("checkpoint.storage必须是带绝对路径的hdfs/file/s3 URI");
        }
        jobName = p.getProperty("job.name", "yx2602-heartbeat-reader").trim();
        if (jobName.isEmpty()) throw new IllegalArgumentException("job.name不能为空");
        parallelism = Math.toIntExact(number(p, "parallelism", 2, 1));
        monitorLogIntervalMs = HeartbeatDiagnostics.interval(p);
        parserMaxAgeMs = number(p, "parser.max.age.ms", 600000, 0);
        parserMaxParallelism = Math.toIntExact(number(p, "parser.max.parallelism", 128, 1));
        if (parserMaxParallelism < parallelism || parserMaxParallelism > 32768) {
            throw new IllegalArgumentException("parser.max.parallelism须不小于parallelism且不超过32768");
        }
        futureSkewMs = number(p, "validation.max.future.skew.ms", 60000, 0);
        checkpointIntervalMs = number(p, "checkpoint.interval.ms", 30000, 1);
        checkpointTimeoutMs = number(p, "checkpoint.timeout.ms", 180000, 1);
        deliveryTimeoutMs = Math.toIntExact(number(p, "kafka.delivery.timeout.ms", 120000, 31000));
        restartAttempts = Math.toIntExact(number(p, "restart.attempts", 10, 1));
        restartDelayMs = number(p, "restart.delay.ms", 10000, 1);
        maxRequestBytes = Math.toIntExact(number(p, "kafka.error.max.request.bytes", 8388608, 1));
        if (checkpointTimeoutMs <= deliveryTimeoutMs) {
            throw new IllegalArgumentException("检查点超时必须大于Kafka发送超时");
        }
        ignite = new IgniteConfig(p, checkpointTimeoutMs);
    }

    public static ReaderConfig load(Path path) throws IOException {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        return from(properties, System::getenv);
    }

    static ReaderConfig from(Properties properties, Function<String, String> environment) {
        Properties expanded = new Properties();
        for (String key : properties.stringPropertyNames()) {
            Matcher matcher = ENV.matcher(properties.getProperty(key));
            StringBuffer value = new StringBuffer();
            while (matcher.find()) {
                String replacement = environment.apply(matcher.group(1));
                if (replacement == null) throw new IllegalArgumentException("缺少环境变量: " + matcher.group(1));
                matcher.appendReplacement(value, Matcher.quoteReplacement(replacement));
            }
            matcher.appendTail(value);
            expanded.setProperty(key, value.toString());
        }
        return new ReaderConfig(expanded);
    }

    public Properties consumerProperties() {
        Properties p = connectionProperties();
        p.setProperty("enable.auto.commit", "false");
        p.setProperty("commit.offsets.on.checkpoint", "true");
        p.setProperty("allow.auto.create.topics", "false");
        // 禁止自动发现新分区；Topic 扩分区后需重新提交，由初始化流程确定新分区起始位点。
        p.setProperty("partition.discovery.interval.ms", "-1");
        return p;
    }

    public Properties producerProperties() {
        Properties p = connectionProperties();
        p.setProperty("acks", "all");
        p.setProperty("enable.idempotence", "true");
        p.setProperty("retries", Integer.toString(Integer.MAX_VALUE));
        p.setProperty("retry.backoff.ms", "1000");
        p.setProperty("max.in.flight.requests.per.connection", "5");
        p.setProperty("request.timeout.ms", "30000");
        p.setProperty("delivery.timeout.ms", Integer.toString(deliveryTimeoutMs));
        p.setProperty("max.block.ms", Integer.toString(deliveryTimeoutMs));
        p.setProperty("linger.ms", "5");
        p.setProperty("max.request.size", Integer.toString(maxRequestBytes));
        return p;
    }

    Properties connectionProperties() {
        Properties copy = new Properties();
        copy.putAll(connectionProperties);
        return copy;
    }

    private static String required(Properties p, String key) {
        String value = p.getProperty(key);
        if (value == null || value.trim().isEmpty()) throw new IllegalArgumentException("缺少配置: " + key);
        return value.trim();
    }

    private static String topic(String name) {
        if (!name.matches("[a-zA-Z0-9._-]{1,249}") || name.equals(".") || name.equals("..")) {
            throw new IllegalArgumentException("必须配置单个合法的Kafka主题名");
        }
        return name;
    }

    private static long number(Properties p, String key, long fallback, long minimum) {
        long value;
        try { value = Long.parseLong(p.getProperty(key, Long.toString(fallback)).trim()); }
        catch (NumberFormatException e) { throw new IllegalArgumentException(key + "必须为整数"); }
        if (value < minimum) throw new IllegalArgumentException(key + "必须不小于" + minimum);
        return value;
    }
}
