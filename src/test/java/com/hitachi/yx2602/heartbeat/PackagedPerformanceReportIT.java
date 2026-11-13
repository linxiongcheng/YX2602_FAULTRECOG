package com.hitachi.yx2602.heartbeat;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class PackagedPerformanceReportIT {
    @TempDir Path directory;

    @ParameterizedTest @ValueSource(strings = {"full-once", "full-watch", "simple-once", "simple-watch"}) @Timeout(25)
    void runsWithOnlyTheJarAndSupportsWatchStartup(String mode) throws Exception {
        boolean watch = mode.endsWith("watch"), simple = mode.startsWith("simple");
        Path input = directory.resolve("monitor.log"), report = directory.resolve("report.md"), log = directory.resolve("process.log");
        Files.writeString(input, "2026-11-10T10:00:00+08:00[Asia/Shanghai] [HEARTBEAT-PERF] stage=ignite_sink job=sample subtask=0/1 attempt=0 window_ms=30000 batch_records=300 committed=300\n");
        List<String> command = new ArrayList<>(Arrays.asList(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dfile.encoding=UTF-8", "-cp", Paths.get("target/heartbeat-reader-0.1.0.jar").toAbsolutePath().toString(),
                "com.hitachi.yx2602.heartbeat.HeartbeatPerformanceReport", "--input", input.toString(),
                "--output", report.toString(), "--minutes", "15", "--as-of-latest"));
        if (watch) command.add("--watch");
        if (simple) command.add("--simple");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            if (watch) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
                while ((!Files.exists(report) || (simple && !Files.readString(log).contains("Ignite写入平均")))
                        && process.isAlive() && System.nanoTime() < deadline) Thread.sleep(50);
                assertTrue(process.isAlive(), Files.readString(log));
            } else {
                assertTrue(process.waitFor(12, TimeUnit.SECONDS));
                assertEquals(0, process.exitValue(), Files.readString(log));
            }
            assertTrue(Files.exists(report), Files.readString(log));
            String text = Files.readString(report);
            if (simple) {
                assertTrue(text.contains("Ignite写入平均：10.000 条单梯更新/秒"), text);
                assertTrue(text.contains("最高：10.000 条单梯更新/秒"), text);
                assertTrue(Files.readString(log).contains("Ignite写入平均"), Files.readString(log));
            } else assertTrue(text.contains("|Ignite写入|单梯更新，非物理行数|10.000|10.000|1|1/1|30.000|"), text);
            assertTrue(text.contains("历史回看"));
        } finally {
            if (process.isAlive()) process.destroyForcibly();
            assertTrue(process.waitFor(5, TimeUnit.SECONDS));
        }
    }
}
