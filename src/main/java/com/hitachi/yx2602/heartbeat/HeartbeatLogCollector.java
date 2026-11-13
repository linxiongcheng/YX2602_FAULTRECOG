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
    private final HeartbeatLogFiles.DailyLog monitorLog;
    private final HeartbeatLogFiles.Cleaner cleaner;
    private final Map<String, Long> positions = new LinkedHashMap<>();

    HeartbeatLogCollector(String baseUrl, String applicationId, Path directory) throws IOException {
        this(baseUrl, applicationId, directory,
                longProperty("heartbeat.collector.monitor.max.bytes", 30L * 1024 * 1024),
                HeartbeatLogFiles.retentionDays());
    }

    HeartbeatLogCollector(String baseUrl, String applicationId, Path directory,
                          long maxBytes, int retentionDays) throws IOException {
        URI uri = URI.create(baseUrl);
        if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) || uri.getHost() == null
                || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("REST URL must be an http(s) base URL");
        }
        if (!applicationId.matches("application_[0-9]+_[0-9]+")) throw new IllegalArgumentException("Invalid Application ID");
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        Files.createDirectories(directory);
        output = directory.resolve("heartbeat-monitor-" + applicationId + ".log");
        monitorLog = new HeartbeatLogFiles.DailyLog(output, maxBytes);
        cleaner = new HeartbeatLogFiles.Cleaner(directory, retentionDays);
        state = directory.resolve(".heartbeat-monitor-" + applicationId + ".offsets.json");
        if (Files.exists(state)) {
            JsonNode saved = JSON.readTree(Files.readAllBytes(state));
            saved.fields().forEachRemaining(entry -> positions.put(entry.getKey(), entry.getValue().asLong()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("--cleanup-logs") && args[1].equals("--output-dir")) {
            System.out.println("removed_log_files=" + new HeartbeatLogFiles.Cleaner(
                    Paths.get(args[2]), HeartbeatLogFiles.retentionDays()).clean());
            return;
        }
        if ((args.length != 6 && args.length != 7) || !args[0].equals("--rest-url") || !args[2].equals("--application-id")
                || !args[4].equals("--output-dir") || (args.length == 7 && !args[6].equals("--once"))) {
            throw new IllegalArgumentException("Usage: --rest-url URL --application-id ID --output-dir DIRECTORY [--once]"
                    + " or --cleanup-logs --output-dir DIRECTORY");
        }
        HeartbeatLogCollector collector = new HeartbeatLogCollector(args[1], args[3], Paths.get(args[5]));
        Path lockPath = collector.state.resolveSibling(collector.state.getFileName() + ".lock");
        try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(lockPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             java.nio.channels.FileLock lock = channel.tryLock()) {
            if (lock == null) throw new IllegalStateException("A collector already owns this application log");
            if (args.length == 7) {
                System.out.println("collected=" + collector.collectOnce());
                collector.cleaner.clean();
                return;
            }
            HeartbeatLogFiles.DailyLog statusLog = new HeartbeatLogFiles.DailyLog(Paths.get(args[5]).resolve("collector-" + args[3] + ".log"),
                    longProperty("heartbeat.collector.status.max.bytes", 10L * 1024 * 1024));
            int failures = 0;
            while (!Thread.currentThread().isInterrupted() && failures < 10) {
                try {
                    collector.monitorLog.rollForToday();
                    statusLog.rollForToday();
                    int removed = collector.cleaner.cleanIfDue();
                    if (removed > 0) statusLog.append(Instant.now() + " removed_log_files=" + removed);
                } catch (IOException cleanup) {
                    // Cleanup trouble must not stop collection of new diagnostics.
                    statusLog.append(Instant.now() + " cleanup_error=" + cleanup.getMessage());
                }
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
        monitorLog.rollForToday();
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
