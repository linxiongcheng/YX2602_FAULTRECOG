package com.hitachi.yx2602.heartbeat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/** Ignite 轻客户端、批量写入及重试配置；凭据不得输出到日志。 */
public final class IgniteConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final Set<String> KEYS = new HashSet<>(Arrays.asList(
            "ignite.addresses", "ignite.username", "ignite.password", "ignite.ssl.enabled",
            "ignite.request.timeout.ms", "ignite.transaction.timeout.ms", "ignite.retry.attempts",
            "ignite.retry.delay.ms", "ignite.batch.size", "ignite.queue.capacity",
            "ignite.batch.linger.ms", "ignite.flush.timeout.ms", "ignite.history.max.records",
            "ignite.history.config.path", "ignite.history.refresh.interval.ms", "max.records"));
    public final String addresses, username, password;
    public final boolean sslEnabled;
    public final int requestTimeoutMs, transactionTimeoutMs, attempts, batchSize, queueCapacity;
    public final long retryDelayMs, lingerMs, flushTimeoutMs;
    public final int historyMaxRecords, historyRefreshIntervalMs;
    public final String historyConfigPath;
    final boolean historyMaxRecordsExplicit;

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
        if (p.containsKey("max.records") && p.containsKey("ignite.history.max.records")) {
            throw new IllegalArgumentException("max.records和ignite.history.max.records只能配置一个");
        }
        historyMaxRecordsExplicit = p.containsKey("max.records") || p.containsKey("ignite.history.max.records");
        historyMaxRecords = historyLimit(integer(p, p.containsKey("max.records") ? "max.records" : "ignite.history.max.records", 10));
        historyRefreshIntervalMs = integer(p, "ignite.history.refresh.interval.ms", 10000);
        if (historyRefreshIntervalMs < 1000) {
            throw new IllegalArgumentException("ignite.history.refresh.interval.ms必须不小于1000");
        }
        historyConfigPath = p.getProperty("ignite.history.config.path", "").trim();
        if (!historyConfigPath.isEmpty()) {
            java.net.URI uri = java.net.URI.create(historyConfigPath);
            if (!("hdfs".equals(uri.getScheme()) || "file".equals(uri.getScheme()))
                    || uri.getPath() == null || !uri.getPath().startsWith("/")
                    || uri.getQuery() != null || uri.getFragment() != null) {
                throw new IllegalArgumentException("ignite.history.config.path必须是绝对hdfs/file URI");
            }
        }
        // 刷出超时须早于检查点超时，同时为事务执行和凑批保留等待时间。
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

    static int historyLimit(int count) {
        if (count < 1 || count > 1000) throw new IllegalArgumentException("历史保留条数必须在1～1000之间");
        return count;
    }

    /** 配置版本严格递增，让刷新较慢的任务不会覆盖已经生效的新策略。 */
    static final class HistoryPolicy {
        final long version;
        final int maxRecords;
        HistoryPolicy(long version, int maxRecords) {
            this.version = version;
            this.maxRecords = historyLimit(maxRecords);
        }
    }

    /** 后台读取共享小文件；运行时错误保留最后有效值，首次加载失败则阻止启动。 */
    static final class HistorySettings implements AutoCloseable {
        private static final java.util.logging.Logger LOG = java.util.logging.Logger.getLogger("heartbeat-history-config");
        private final String path;
        private final java.util.concurrent.ScheduledExecutorService executor;
        private volatile HistoryPolicy current;
        private volatile boolean lastRefreshFailed;

        HistorySettings(IgniteConfig config) {
            path = config.historyConfigPath;
            if (path.isEmpty()) {
                current = new HistoryPolicy(0, config.historyMaxRecords);
                executor = null;
            } else {
                try { current = readPolicy(path); }
                catch (Exception error) { throw new IllegalArgumentException("无法加载历史动态配置，拒绝以默认值启动", error); }
                executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(task -> {
                    Thread thread = new Thread(task, "heartbeat-history-config");
                    thread.setDaemon(true);
                    return thread;
                });
                executor.scheduleWithFixedDelay(this::refresh, config.historyRefreshIntervalMs,
                        config.historyRefreshIntervalMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                LOG.info("历史动态配置已加载: version=" + current.version + ", max.records=" + current.maxRecords);
            }
        }

        HistoryPolicy current() { return current; }
        boolean lastRefreshFailed() { return lastRefreshFailed; }

        synchronized void refresh() {
            if (path.isEmpty()) return;
            try {
                HistoryPolicy candidate = readPolicy(path);
                if (candidate.version < current.version
                        || (candidate.version == current.version && candidate.maxRecords != current.maxRecords)) {
                    throw new IllegalArgumentException("修改保留条数必须增加version，且version不能倒退");
                }
                if (candidate.version > current.version || lastRefreshFailed) {
                    LOG.info("历史动态配置生效: version=" + candidate.version + ", max.records=" + candidate.maxRecords);
                }
                current = candidate;
                lastRefreshFailed = false;
            } catch (Exception error) {
                if (!lastRefreshFailed) LOG.warning("历史配置刷新失败，继续使用version=" + current.version
                        + ", max.records=" + current.maxRecords + "；原因=" + error.getClass().getSimpleName());
                lastRefreshFailed = true;
            }
        }

        static HistoryPolicy readPolicy(String location) throws java.io.IOException {
            java.net.URI uri = java.net.URI.create(location);
            byte[] bytes;
            if ("file".equals(uri.getScheme())) {
                try (java.io.InputStream stream = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(uri))) {
                    bytes = readSmallFile(stream);
                }
            } else {
                bytes = SharedFile.read(location);
            }
            Properties p = new Properties() {
                @Override public synchronized Object put(Object key, Object value) {
                    if (containsKey(key)) throw new IllegalArgumentException("动态配置不能包含重复键");
                    return super.put(key, value);
                }
            };
            try (java.io.Reader reader = new java.io.InputStreamReader(new java.io.ByteArrayInputStream(bytes),
                    java.nio.charset.StandardCharsets.UTF_8.newDecoder()
                            .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT))) {
                p.load(reader);
            }
            if (!p.stringPropertyNames().equals(new HashSet<>(Arrays.asList("version", "max.records")))) {
                throw new IllegalArgumentException("动态文件必须且只能包含version和max.records");
            }
            long version = Long.parseLong(p.getProperty("version").trim());
            if (version <= 0) throw new IllegalArgumentException("动态配置version必须为正整数");
            return new HistoryPolicy(version, integer(p, "max.records", 10));
        }

        private static byte[] readSmallFile(java.io.InputStream stream) throws java.io.IOException {
            byte[] bytes = stream.readNBytes(8193);
            if (bytes.length > 8192) throw new java.io.IOException("动态配置文件不能超过8KiB");
            return bytes;
        }

        @Override public void close() { if (executor != null) executor.shutdownNow(); }

        // 将Flink文件系统加载延后到实际使用HDFS时，独立客户端的本地配置无需Hadoop环境。
        private static final class SharedFile {
            static byte[] read(String location) throws java.io.IOException {
                org.apache.flink.core.fs.Path file = new org.apache.flink.core.fs.Path(location);
                try (java.io.InputStream stream = file.getFileSystem().open(file)) {
                    return readSmallFile(stream);
                }
            }

            static boolean exists(String location) throws java.io.IOException {
                org.apache.flink.core.fs.Path file = new org.apache.flink.core.fs.Path(location);
                return file.getFileSystem().exists(file);
            }
        }
    }

    /** 在提交节点读取原始local.properties；只向共享文件发布条数和自动生成的版本。 */
    static final class HistoryPublisher implements AutoCloseable {
        private static final java.util.logging.Logger LOG = java.util.logging.Logger.getLogger("heartbeat-history-publisher");
        private final java.nio.file.Path source;
        private final String destination;
        private final int intervalMs;
        private final java.nio.channels.FileChannel lockChannel;
        private final java.nio.channels.FileLock lock;
        private long lastVersion;
        private boolean failed;

        HistoryPublisher(java.nio.file.Path source) throws java.io.IOException {
            this.source = source.toRealPath();
            IgniteConfig config = ReaderConfig.load(this.source).ignite;
            requirePublishConfig(config);
            destination = config.historyConfigPath;
            if ("file".equals(java.net.URI.create(destination).getScheme())
                    && java.nio.file.Files.exists(java.nio.file.Paths.get(java.net.URI.create(destination)))
                    && java.nio.file.Files.isSameFile(java.nio.file.Paths.get(java.net.URI.create(destination)), this.source)) {
                throw new IllegalArgumentException("动态文件不能覆盖local.properties");
            }
            intervalMs = config.historyRefreshIntervalMs;
            lockChannel = java.nio.channels.FileChannel.open(this.source.resolveSibling(this.source.getFileName() + ".history-sync.lock"),
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE);
            java.nio.channels.FileLock acquired;
            try {
                acquired = lockChannel.tryLock();
                if (acquired == null) throw new java.io.IOException("该local.properties已有配置同步进程");
            } catch (RuntimeException | java.io.IOException error) {
                lockChannel.close();
                throw error;
            }
            lock = acquired;
        }

        private static void requirePublishConfig(IgniteConfig config) {
            if (config.historyConfigPath.isEmpty() || !config.historyMaxRecordsExplicit) {
                throw new IllegalArgumentException("配置同步必须显式设置max.records和ignite.history.config.path");
            }
        }

        void sync() throws java.io.IOException, InterruptedException {
            IgniteConfig desired = ReaderConfig.load(source).ignite;
            requirePublishConfig(desired);
            if (!destination.equals(desired.historyConfigPath)) {
                throw new IllegalArgumentException("同步期间不能修改共享配置路径，请重启同步进程");
            }
            java.net.URI uri = java.net.URI.create(destination);
            boolean local = "file".equals(uri.getScheme());
            boolean exists = local ? java.nio.file.Files.exists(java.nio.file.Paths.get(uri))
                    : HistorySettings.SharedFile.exists(destination);
            HistoryPolicy previous = exists ? HistorySettings.readPolicy(destination) : null;
            if (previous != null) {
                lastVersion = Math.max(lastVersion, previous.version);
                if (previous.maxRecords == desired.historyMaxRecords) return;
            }
            long version = Math.max(System.currentTimeMillis(), Math.addExact(lastVersion, 1));
            // 先记住已分配版本；即使发布成功但响应丢失，后续也不会分配更低版本。
            lastVersion = version;
            byte[] bytes = ("# Generated automatically; edit max.records in local.properties.\nversion=" + version
                    + "\nmax.records=" + desired.historyMaxRecords + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            if (local) {
                java.nio.file.Path target = java.nio.file.Paths.get(uri);
                java.nio.file.Files.createDirectories(target.getParent());
                java.nio.file.Path temporary = java.nio.file.Files.createTempFile(target.getParent(), ".heartbeat-history-", ".tmp");
                try {
                    java.nio.file.Files.write(temporary, bytes);
                    java.nio.file.Files.move(temporary, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                            java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                } finally { java.nio.file.Files.deleteIfExists(temporary); }
            } else {
                String parent = new org.apache.flink.core.fs.Path(destination).getParent().toString();
                hdfs(null, "-mkdir", "-p", parent);
                // Hadoop FsShell默认先写临时文件再发布；不用-d，不把整个local.properties上传。
                hdfs(bytes, "-put", "-f", "-", destination);
            }
            LOG.info("历史配置已发布: version=" + version + ", max.records=" + desired.historyMaxRecords);
        }

        void watch() throws java.io.IOException, InterruptedException {
            sync();
            LOG.info("正在监视local.properties中的max.records，刷新间隔=" + intervalMs + "ms");
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(intervalMs);
                try {
                    sync();
                    if (failed) LOG.info("历史配置同步已恢复");
                    failed = false;
                } catch (java.io.IOException | RuntimeException error) {
                    if (!failed) LOG.warning("历史配置同步失败，保留共享文件中的有效值；原因=" + error.getClass().getSimpleName());
                    failed = true;
                }
            }
        }

        private static void hdfs(byte[] input, String... arguments) throws java.io.IOException, InterruptedException {
            java.util.List<String> command = new java.util.ArrayList<>(Arrays.asList("hdfs", "dfs"));
            command.addAll(Arrays.asList(arguments));
            Process process = new ProcessBuilder(command).redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.DISCARD).start();
            try {
                try (java.io.OutputStream stream = process.getOutputStream()) { if (input != null) stream.write(input); }
                if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)) {
                    throw new java.io.IOException("HDFS配置发布命令超时");
                }
                if (process.exitValue() != 0) throw new java.io.IOException("HDFS配置发布失败，退出码=" + process.exitValue());
            } finally { if (process.isAlive()) process.destroyForcibly(); }
        }

        @Override public void close() throws java.io.IOException {
            try { lock.release(); } finally { lockChannel.close(); }
        }
    }
}
