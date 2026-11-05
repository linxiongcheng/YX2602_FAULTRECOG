package com.hitachi.yx2602.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class HeartbeatDiagnosticsTest {
    @Test void windowsSeparateWorkAndEmitTimeAndShowInflightOperationWithoutCompletion() {
        AtomicLong clock = new AtomicLong();
        List<String> logs = new ArrayList<>();
        HeartbeatDiagnostics monitor = new HeartbeatDiagnostics("split", "subtask=0/3 attempt=1", 1000, clock::get, logs::add);
        monitor.add("input", 2);
        long work = monitor.begin("split");
        clock.set(TimeUnit.MILLISECONDS.toNanos(3)); monitor.end("split", work);
        long emit = monitor.begin("emit");
        clock.set(TimeUnit.MILLISECONDS.toNanos(43)); monitor.end("emit", emit);
        monitor.observedOffset(2, 42);
        monitor.gauge("queued", () -> 7);
        clock.set(TimeUnit.SECONDS.toNanos(1));
        String first = monitor.snapshot();
        assertTrue(first.contains("input=2 input_per_s=2.000"), first);
        assertTrue(first.contains("split_samples=1 split_total_ms=3.000 split_avg_ms=3.000"), first);
        assertTrue(first.contains("emit_samples=1 emit_total_ms=40.000 emit_avg_ms=40.000"), first);
        assertTrue(first.contains("queued=7")); assertTrue(first.contains("observed_offset_p2=42"));
        long started = monitor.begin("bulk_write");
        clock.set(TimeUnit.MILLISECONDS.toNanos(1500));
        String active = monitor.snapshot();
        assertTrue(active.contains("bulk_write_samples=0"), active);
        assertTrue(active.contains("bulk_write_active_ms=500"), active);
        assertTrue(active.contains("input=0 input_per_s=0.000"), active);
        clock.set(TimeUnit.SECONDS.toNanos(2)); monitor.end("bulk_write", started);
        String finished = monitor.snapshot();
        assertTrue(finished.contains("bulk_write_samples=1 bulk_write_total_ms=1000.000"), finished);
        assertTrue(finished.contains("bulk_write_active_ms=0"), finished);
        monitor.checkpoint("completed", 8);
        assertTrue(logs.get(0).contains("[HEARTBEAT-CHECKPOINT] stage=split subtask=0/3 attempt=1 event=completed checkpoint_id=8"));
        monitor.close();
    }

    @Test void periodicReportContinuesWhileWriterAndProducerAreBlocked() throws Exception {
        Properties properties = ReaderConfigTest.properties();
        properties.setProperty("ignite.batch.size", "1");
        properties.setProperty("ignite.queue.capacity", "1");
        IgniteConfig config = ReaderConfig.from(properties, key -> null).ignite;
        LinkedBlockingQueue<String> logs = new LinkedBlockingQueue<>();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AsyncHeartbeatWriter.Store store = new AsyncHeartbeatWriter.Store() {
            public int write(List<IgniteHeartbeatStore.Row> rows) throws Exception {
                entered.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
                return rows.size();
            }
            public void close() { }
        };
        try (HeartbeatDiagnostics monitor = new HeartbeatDiagnostics("ignite_sink", "subtask=1/3 attempt=0",
                1000, System::nanoTime, logs::add);
             AsyncHeartbeatWriter writer = new AsyncHeartbeatWriter(config, store, monitor)) {
            monitor.start();
            writer.submit(IgniteStoreTest.update(1, 1, 1));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            writer.submit(IgniteStoreTest.update(2, 1, 1));
            CompletableFuture<Void> blocked = CompletableFuture.runAsync(() -> {
                try { writer.submit(IgniteStoreTest.update(3, 1, 1)); }
                catch (Exception error) { throw new RuntimeException(error); }
            });
            try {
                assertThrows(java.util.concurrent.TimeoutException.class, () -> blocked.get(100, TimeUnit.MILLISECONDS));
                String line = logs.poll(4, TimeUnit.SECONDS);
                assertNotNull(line);
                assertTrue(line.contains("queued=1"), line);
                assertTrue(line.contains("committed_total=0"), line);
                assertTrue(line.matches(".*batch_write_active_ms=[1-9][0-9]* .*"), line);
                assertTrue(line.matches(".*enqueue_active_ms=[1-9][0-9]* .*"), line);
            } finally { release.countDown(); }
            blocked.get(5, TimeUnit.SECONDS);
            writer.flush();
            String done = monitor.snapshot();
            assertTrue(done.contains("committed_total=3"), done);
            assertTrue(done.contains("queued=0"), done);
        } finally { release.countDown(); }
    }

    @Test void disabledMonitorAndBrokenLogConsumerDoNotFailBusinessThread() {
        List<String> logs = new ArrayList<>();
        try (HeartbeatDiagnostics disabled = new HeartbeatDiagnostics("test", "", 0, System::nanoTime, logs::add)) {
            disabled.start(); disabled.add("input", 1); disabled.begin("work"); disabled.end("work", 0);
            disabled.checkpoint("completed", 1);
        }
        assertTrue(logs.isEmpty());
        HeartbeatDiagnostics broken = new HeartbeatDiagnostics("test", "", 1000,
                System::nanoTime, line -> { throw new IllegalStateException("log destination failed"); });
        assertDoesNotThrow(() -> broken.checkpoint("completed", 1));
        assertDoesNotThrow(broken::close);
    }

    @ParameterizedTest @ValueSource(strings = {"0", "1000", "30000", "3600000"})
    void intervalReachesReaderAndSink(String value) {
        Properties properties = ReaderConfigTest.properties();
        properties.setProperty("monitor.log.interval.ms", value);
        ReaderConfig config = ReaderConfig.from(properties, key -> null);
        assertEquals(Long.parseLong(value), config.monitorLogIntervalMs);
        assertEquals(config.monitorLogIntervalMs, config.ignite.monitorLogIntervalMs);
    }

    @ParameterizedTest @ValueSource(strings = {"-1", "1", "999", "3600001", "abc", "9223372036854775808"})
    void invalidIntervalRejectedAtStartup(String value) {
        Properties properties = ReaderConfigTest.properties();
        properties.setProperty("monitor.log.interval.ms", value);
        assertThrows(IllegalArgumentException.class, () -> ReaderConfig.from(properties, key -> null));
    }
}
