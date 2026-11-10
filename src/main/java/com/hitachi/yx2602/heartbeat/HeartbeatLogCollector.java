package com.hitachi.yx2602.heartbeat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Submission-node collector: read existing Flink REST logs, append only new monitor events locally. */
public final class HeartbeatLogCollector {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern EVENT = Pattern.compile("event_id=([a-f0-9-]+):([0-9]+) ");
    private final String baseUrl;
    private final Path output, state;
    private final RollingFile monitorLog;
    private final Map<String, Long> positions = new LinkedHashMap<>();

    HeartbeatLogCollector(String baseUrl, String applicationId, Path directory) throws IOException {
        this(baseUrl, applicationId, directory,
                longProperty("heartbeat.collector.monitor.max.bytes", 30L * 1024 * 1024),
                backupProperty("heartbeat.collector.monitor.backup.count", 0));
    }

    HeartbeatLogCollector(String baseUrl, String applicationId, Path directory,
                          long maxBytes, int backups) throws IOException {
        URI uri = URI.create(baseUrl);
        if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) || uri.getHost() == null
                || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("REST URL must be an http(s) base URL");
        }
        if (!applicationId.matches("application_[0-9]+_[0-9]+")) throw new IllegalArgumentException("Invalid Application ID");
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        Files.createDirectories(directory);
        output = directory.resolve("heartbeat-monitor-" + applicationId + ".log");
        monitorLog = new RollingFile(output, maxBytes, backups);
        state = directory.resolve(".heartbeat-monitor-" + applicationId + ".offsets.json");
        if (Files.exists(state) && (Files.exists(output) || Files.exists(monitorLog.backup(1)))) {
            JsonNode saved = JSON.readTree(Files.readAllBytes(state));
            saved.fields().forEachRemaining(entry -> positions.put(entry.getKey(), entry.getValue().asLong()));
        }
    }

    public static void main(String[] args) throws Exception {
        if ((args.length != 6 && args.length != 7) || !args[0].equals("--rest-url") || !args[2].equals("--application-id")
                || !args[4].equals("--output-dir") || (args.length == 7 && !args[6].equals("--once"))) {
            throw new IllegalArgumentException("Usage: --rest-url URL --application-id ID --output-dir DIRECTORY [--once]");
        }
        HeartbeatLogCollector collector = new HeartbeatLogCollector(args[1], args[3], Paths.get(args[5]));
        Path lockPath = collector.state.resolveSibling(collector.state.getFileName() + ".lock");
        try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(lockPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             java.nio.channels.FileLock lock = channel.tryLock()) {
            if (lock == null) throw new IllegalStateException("A collector already owns this application log");
            if (args.length == 7) {
                System.out.println("collected=" + collector.collectOnce());
                return;
            }
            RollingFile statusLog = new RollingFile(Paths.get(args[5]).resolve("collector-" + args[3] + ".log"),
                    longProperty("heartbeat.collector.status.max.bytes", 10L * 1024 * 1024),
                    backupProperty("heartbeat.collector.status.backup.count", 0));
            int failures = 0;
            while (!Thread.currentThread().isInterrupted() && failures < 10) {
                try {
                    int added = collector.collectOnce();
                    failures = 0;
                    statusLog.append(Instant.now() + " collected=" + added + " output=" + collector.output);
                } catch (IOException error) {
                    failures++;
                    statusLog.append(Instant.now() + " collector_retry=" + failures + "/10 error="
                            + error.getClass().getSimpleName() + " message=" + error.getMessage());
                }
                if (failures < 10) Thread.sleep(30000);
            }
            if (failures == 10) throw new IOException("Flink REST unavailable for 10 consecutive polls; collector stopped");
        }
    }

    int collectOnce() throws IOException {
        byte[] listing = get("/taskmanagers", 1024 * 1024);
        if (listing == null) throw new IOException("TaskManager list unavailable");
        JsonNode managers = JSON.readTree(listing).path("taskmanagers");
        if (!managers.isArray()) throw new IOException("Unexpected TaskManager response");
        int added = 0;
        IOException pending = null;
        for (JsonNode manager : managers) {
            String id = manager.path("id").asText();
            if (id.isEmpty()) continue;
            String encoded = java.net.URLEncoder.encode(id, StandardCharsets.UTF_8).replace("+", "%20");
            String logPath = "/taskmanagers/" + encoded + "/logs";
            Set<String> available = new HashSet<>();
            try {
                byte[] files = get(logPath, 1024 * 1024);
                if (files == null) continue; // TaskManager may have disappeared since the first listing.
                JsonNode logs = JSON.readTree(files).path("logs");
                if (!logs.isArray()) throw new IOException("Unexpected TaskManager log listing: " + id);
                for (JsonNode log : logs) available.add(log.path("name").asText());
            } catch (IOException error) {
                pending = error;
                continue;
            }
            // Read the previous generation before the current one to preserve each JVM's event sequence.
            // Flink 1.20 returns HTTP 500 (and logs an ERROR) for a missing named file.
            // The rotated file, and initially even the current file, may not exist yet.
            for (String name : new String[]{"heartbeat-monitor.log.1", "heartbeat-monitor.log"}) {
                if (!available.contains(name)) continue;
                try {
                    byte[] data = get(logPath + "/" + name, 2 * 1024 * 1024);
                    if (data != null) added += appendEvents(new String(data, StandardCharsets.UTF_8));
                } catch (IOException error) { pending = error; }
            }
        }
        savePositions();
        if (pending != null) throw pending;
        return added;
    }

    int appendEvents(String text) throws IOException {
        int added = 0;
        int complete = text.lastIndexOf('\n');
        if (complete < 0) return 0;
        for (String line : text.substring(0, complete).split("\n")) {
            Matcher event = EVENT.matcher(line);
            if (!event.find() || !(line.contains("[HEARTBEAT-PERF]") || line.contains("[HEARTBEAT-CHECKPOINT]"))) continue;
            String stream = event.group(1);
            long sequence;
            try { sequence = Long.parseLong(event.group(2)); }
            catch (NumberFormatException invalid) { continue; }
            if (sequence <= positions.getOrDefault(stream, 0L)) continue;
            // Complete the append before advancing the cursor; rotating does not reset positions.
            monitorLog.append(line);
            positions.put(stream, sequence);
            added++;
        }
        return added;
    }

    private static long longProperty(String name, long defaultValue) {
        long value = Long.parseLong(System.getProperty(name, Long.toString(defaultValue)));
        if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
        return value;
    }

    private static int backupProperty(String name, int defaultValue) {
        long value = Long.parseLong(System.getProperty(name, Integer.toString(defaultValue)));
        if (value < 0 || value > 100) throw new IllegalArgumentException(name + " must be 0..100");
        return (int) value;
    }

    /** Single collector owns these files under its application lock; no persistent open handles. */
    static final class RollingFile {
        private final Path file;
        private final long maxBytes;
        private final int backups;
        private boolean cleanupPending = true;

        RollingFile(Path file, long maxBytes, int backups) {
            if (maxBytes <= 0 || backups < 0 || backups > 100) {
                throw new IllegalArgumentException("Log max bytes must be positive; backups must be 0..100");
            }
            this.file = file; this.maxBytes = maxBytes; this.backups = backups;
        }

        Path backup(int number) { return file.resolveSibling(file.getFileName() + "." + number); }

        void append(String line) throws IOException {
            byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
            if (cleanupPending) {
                // Only this application's reserved numbered backups, under its collector lock.
                for (int i = backups + 1; i <= 100; i++) Files.deleteIfExists(backup(i));
                cleanupPending = false;
            }
            long size = Files.exists(file) ? Files.size(file) : 0;
            if (size > 0 && (size >= maxBytes || bytes.length > maxBytes - size)) {
                if (backups == 0) {
                    Files.write(file, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                    return;
                }
                for (int i = backups - 1; i >= 1; i--) {
                    if (Files.exists(backup(i))) {
                        Files.move(backup(i), backup(i + 1), StandardCopyOption.REPLACE_EXISTING);
                    }
                }
                Files.move(file, backup(1), StandardCopyOption.REPLACE_EXISTING);
            }
            // Keep complete records even if a single unusually long line exceeds the limit.
            Files.write(file, bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    private void savePositions() throws IOException {
        Path temporary = state.resolveSibling(state.getFileName() + ".next");
        Files.write(temporary, JSON.writeValueAsBytes(positions));
        Files.move(temporary, state, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private byte[] get(String path, int limit) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) URI.create(baseUrl + path).toURL().openConnection();
        connection.setConnectTimeout(3000); connection.setReadTimeout(5000);
        connection.setRequestProperty("Accept", "application/json, text/plain");
        try {
            int status = connection.getResponseCode();
            if (status == 404) return null;
            if (status != 200) throw new IOException("Flink HTTP status " + status);
            try (InputStream input = connection.getInputStream()) {
                byte[] data = input.readNBytes(limit + 1);
                if (data.length > limit) throw new IOException("Flink response exceeded limit");
                return data;
            }
        } finally { connection.disconnect(); }
    }
}
