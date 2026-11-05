package com.hitachi.yx2602.heartbeat;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatLogCollectorTest {
    @TempDir Path directory;
    private static String line(String stream, int sequence, String stage) {
        return "2026-11-05T12:00:00+08:00 event_id=" + stream + ":" + sequence
                + " [HEARTBEAT-PERF] stage=" + stage + " input=10\n";
    }

    @Test @org.junit.jupiter.api.Timeout(45)
    void readsDedicatedMonitorFileThroughActualFlinkRestEndpoint() throws Exception {
        Path container = Files.createDirectory(directory.resolve("container"));
        Files.writeString(container.resolve("taskmanager.log"), "regular Flink log\n");
        Files.writeString(container.resolve("heartbeat-monitor.log.1"), line("aaaa", 1, "validation"));
        Files.writeString(container.resolve("heartbeat-monitor.log"), line("aaaa", 2, "ignite_sink"));
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.set(org.apache.flink.configuration.TaskManagerOptions.TASK_MANAGER_LOG_PATH,
                container.resolve("taskmanager.log").toAbsolutePath().toString());
        org.apache.flink.runtime.minicluster.MiniClusterConfiguration settings =
                new org.apache.flink.runtime.minicluster.MiniClusterConfiguration.Builder()
                        .setConfiguration(config).setNumTaskManagers(1).setNumSlotsPerTaskManager(1).build();
        try (org.apache.flink.runtime.minicluster.MiniCluster cluster = new org.apache.flink.runtime.minicluster.MiniCluster(settings)) {
            cluster.start();
            HeartbeatLogCollector collector = new HeartbeatLogCollector(cluster.getRestAddress().get().toString(),
                    "application_123_5", directory.resolve("collected"));
            int added = 0;
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(15);
            while (added == 0 && System.nanoTime() < deadline) {
                added = collector.collectOnce();
                if (added == 0) Thread.sleep(100);
            }
            assertEquals(2, added);
            assertEquals(0, collector.collectOnce());
        }
        // Flink 1.20's named-log upload leaves FileInputStream cleanup to the JVM cleaner.
        // Windows disallows deleting those fixtures until that cleanup runs.
        long cleanupDeadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        for (String name : new String[]{"heartbeat-monitor.log", "heartbeat-monitor.log.1"}) {
            while (true) {
                System.gc();
                try { Files.deleteIfExists(container.resolve(name)); break; }
                catch (java.nio.file.FileSystemException busy) {
                    if (System.nanoTime() >= cleanupDeadline) throw busy;
                    Thread.sleep(50);
                }
            }
        }
    }

    @Test void collectsMultipleManagersRotatedLogsAndResumesWithoutDuplicates() throws Exception {
        Map<String, String> responses = new ConcurrentHashMap<>();
        responses.put("/taskmanagers", "{\"taskmanagers\":[{\"id\":\"tm1\"},{\"id\":\"tm2\"}]}");
        responses.put("/taskmanagers/tm1/logs/heartbeat-monitor.log.1", line("aaaa", 1, "validation"));
        responses.put("/taskmanagers/tm1/logs/heartbeat-monitor.log", line("aaaa", 2, "split"));
        responses.put("/taskmanagers/tm2/logs/heartbeat-monitor.log", line("bbbb", 1, "ignite_sink"));
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String text = responses.get(exchange.getRequestURI().getPath());
            byte[] bytes = (text == null ? "not found" : text).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(text == null ? 404 : 200, bytes.length);
            try (java.io.OutputStream output = exchange.getResponseBody()) { output.write(bytes); }
        });
        server.start();
        try {
            String url = "http://127.0.0.1:" + server.getAddress().getPort();
            HeartbeatLogCollector collector = new HeartbeatLogCollector(url, "application_123_4", directory);
            assertEquals(3, collector.collectOnce());
            assertEquals(0, collector.collectOnce());
            responses.put("/taskmanagers/tm1/logs/heartbeat-monitor.log.1", line("aaaa", 1, "validation") + line("aaaa", 2, "split"));
            responses.put("/taskmanagers/tm1/logs/heartbeat-monitor.log", line("aaaa", 3, "ordering"));
            assertEquals(1, collector.collectOnce());
            HeartbeatLogCollector restored = new HeartbeatLogCollector(url, "application_123_4", directory);
            assertEquals(0, restored.collectOnce());
            String partial = line("aaaa", 4, "split").stripTrailing();
            responses.put("/taskmanagers/tm1/logs/heartbeat-monitor.log", partial);
            assertEquals(0, restored.collectOnce());
            responses.put("/taskmanagers/tm1/logs/heartbeat-monitor.log", partial + "\n");
            assertEquals(1, restored.collectOnce());
            Path output = directory.resolve("heartbeat-monitor-application_123_4.log");
            assertEquals(5, Files.readAllLines(output).size());
            responses.remove("/taskmanagers");
            assertThrows(IOException.class, restored::collectOnce);
        } finally { server.stop(0); }
    }

    @Test void rejectsNonEventsAndNeverAdvancesCursorForPartialLine() throws Exception {
        HeartbeatLogCollector collector = new HeartbeatLogCollector("http://localhost:8081", "application_1_1", directory);
        assertEquals(0, collector.appendEvents("unrelated log\n"));
        assertEquals(0, collector.appendEvents("event_id=aaaa:1 unrelated content\n"));
        assertEquals(0, collector.appendEvents(line("aaaa", 1, "split").stripTrailing()));
        assertEquals(1, collector.appendEvents(line("aaaa", 1, "split")));
        assertEquals(0, collector.appendEvents(line("aaaa", 1, "split")));
    }

    @Test void containerLogRotationPreservesCurrentAndPreviousGenerations() throws Exception {
        Path file = directory.resolve("heartbeat-monitor.log");
        HeartbeatDiagnostics.ContainerFile.append(file, "first\n", 10);
        HeartbeatDiagnostics.ContainerFile.append(file, "second\n", 10);
        assertEquals("first\n", Files.readString(directory.resolve("heartbeat-monitor.log.1")));
        assertEquals("second\n", Files.readString(file));
        HeartbeatDiagnostics.ContainerFile.append(file, "third\n", 10);
        assertEquals("second\n", Files.readString(directory.resolve("heartbeat-monitor.log.1")));
        assertEquals("third\n", Files.readString(file));
    }

    @Test void rejectsUnsafeUrlAndApplicationPath() {
        assertThrows(IllegalArgumentException.class,
                () -> new HeartbeatLogCollector("file:///tmp", "application_1_1", directory));
        assertThrows(IllegalArgumentException.class,
                () -> new HeartbeatLogCollector("http://localhost", "../other", directory));
    }
}
