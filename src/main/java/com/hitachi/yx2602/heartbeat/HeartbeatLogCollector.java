package com.hitachi.yx2602.heartbeat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
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
    private final Map<String, Long> positions = new LinkedHashMap<>();

    HeartbeatLogCollector(String baseUrl, String applicationId, Path directory) throws IOException {
        URI uri = URI.create(baseUrl);
        if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) || uri.getHost() == null
                || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("REST URL must be an http(s) base URL");
        }
        if (!applicationId.matches("application_[0-9]+_[0-9]+")) throw new IllegalArgumentException("Invalid Application ID");
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        Files.createDirectories(directory);
        output = directory.resolve("heartbeat-monitor-" + applicationId + ".log");
        state = directory.resolve(".heartbeat-monitor-" + applicationId + ".offsets.json");
        if (Files.exists(state) && Files.exists(output)) {
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
            int failures = 0;
            while (!Thread.currentThread().isInterrupted() && failures < 10) {
                try {
                    int added = collector.collectOnce();
                    failures = 0;
                    System.out.println(Instant.now() + " collected=" + added + " output=" + collector.output);
                } catch (IOException error) {
                    failures++;
                    System.err.println(Instant.now() + " collector_retry=" + failures + "/10 error="
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
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            for (String line : text.substring(0, complete).split("\n")) {
                Matcher event = EVENT.matcher(line);
                if (!event.find() || !(line.contains("[HEARTBEAT-PERF]") || line.contains("[HEARTBEAT-CHECKPOINT]"))) continue;
                String stream = event.group(1);
                long sequence;
                try { sequence = Long.parseLong(event.group(2)); }
                catch (NumberFormatException invalid) { continue; }
                if (sequence <= positions.getOrDefault(stream, 0L)) continue;
                writer.write(line); writer.newLine();
                // Flush before advancing the cursor so a failed local write is retried.
                writer.flush();
                positions.put(stream, sequence);
                added++;
            }
        }
        return added;
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
