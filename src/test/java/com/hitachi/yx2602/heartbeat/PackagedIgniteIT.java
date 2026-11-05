package com.hitachi.yx2602.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class PackagedIgniteIT {
    @TempDir Path directory;

    @Test @Timeout(30) void packagedLogCollectorRunsWithoutFlinkLibraries() throws Exception {
        com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(
                new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String text = path.equals("/taskmanagers") ? "{\"taskmanagers\":[{\"id\":\"tm1\"}]}"
                    : path.endsWith("/heartbeat-monitor.log")
                    ? "event_id=aaaa:1 [HEARTBEAT-PERF] stage=ignite_sink committed=10\n" : null;
            byte[] data = (text == null ? "not found" : text).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(text == null ? 404 : 200, data.length);
            try (java.io.OutputStream output = exchange.getResponseBody()) { output.write(data); }
        });
        server.start();
        Path log = directory.resolve("collector.log"), output = directory.resolve("logs");
        try {
            Process process = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", Paths.get("target/heartbeat-reader-0.1.0.jar").toAbsolutePath().toString(),
                    HeartbeatLogCollector.class.getName(), "--rest-url", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "--application-id", "application_1_1", "--output-dir", output.toString(), "--once")
                    .redirectErrorStream(true).redirectOutput(log.toFile()).start();
            try {
                assertTrue(process.waitFor(20, TimeUnit.SECONDS));
                assertEquals(0, process.exitValue(), Files.readString(log));
                assertTrue(Files.readString(output.resolve("heartbeat-monitor-application_1_1.log")).contains("committed=10"));
            } finally { if (process.isAlive()) process.destroyForcibly(); }
        } finally { server.stop(0); }
    }

    @Test @Timeout(30) void packagedEntryGeneratesHistoryPolicyWithoutManualVersion() throws Exception {
        Path source = directory.resolve("local.properties"), target = directory.resolve("history.properties");
        java.util.Properties properties = ReaderConfigTest.properties();
        properties.setProperty("max.records", "20");
        properties.setProperty("ignite.history.config.path", target.toUri().toString());
        try (java.io.Writer writer = Files.newBufferedWriter(source, StandardCharsets.UTF_8)) { properties.store(writer, null); }
        Path log = directory.resolve("publisher.log");
        Process process = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dfile.encoding=UTF-8", "-jar", Paths.get("target/heartbeat-reader-0.1.0.jar").toAbsolutePath().toString(),
                "--config", source.toString(), "--publish-history-config")
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(process.waitFor(20, TimeUnit.SECONDS));
            assertEquals(0, process.exitValue(), new String(Files.readAllBytes(log), StandardCharsets.UTF_8));
            IgniteConfig.HistoryPolicy actual = IgniteConfig.HistorySettings.readPolicy(target.toUri().toString());
            assertEquals(20, actual.maxRecords);
            assertTrue(actual.version > 0);
        } finally { if (process.isAlive()) process.destroyForcibly(); }
    }

    @Test @Timeout(90) void shadedArtifactConnectsWritesAndReadsActualIgnite() throws Exception {
        IgniteStoreTest.directory = directory;
        try {
            IgniteStoreTest.provision();
            String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
            String classpath = Paths.get("target/heartbeat-reader-0.1.0.jar").toAbsolutePath()
                    + File.pathSeparator + Paths.get("target/test-classes").toAbsolutePath();
            Path log = Paths.get("target/packaged-ignite-smoke.log");
            Process process = new ProcessBuilder(java,
                    "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                    "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED",
                    "--add-opens=java.base/java.io=ALL-UNNAMED", "-cp", classpath,
                    PackagedIgniteClient.class.getName(), "127.0.0.1:10990")
                    .redirectErrorStream(true).redirectOutput(log.toFile()).start();
            try {
                assertTrue(process.waitFor(45, TimeUnit.SECONDS), "packaged client timed out");
                String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
                assertEquals(0, process.exitValue(), output);
                assertTrue(output.contains("PACKAGED_IGNITE_SMOKE_OK"), output);
            } finally { if (process.isAlive()) process.destroyForcibly(); }
        } finally { IgniteStoreTest.stopServer(); }
    }

}
