package com.hitachi.yx2602.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class AsyncHeartbeatWriterTest {
    private IgniteConfig settings() {
        Properties p = ReaderConfigTest.properties();
        p.setProperty("ignite.batch.size", "1");
        p.setProperty("ignite.queue.capacity", "1");
        return ReaderConfig.from(p, key -> null).ignite;
    }

    @Test void checkpointBarrierWaitsForCommitAndObjectReuseDoesNotCorruptQueuedRows() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        List<IgniteHeartbeatStore.Row> committed = new ArrayList<>();
        AsyncHeartbeatWriter.Store store = new AsyncHeartbeatWriter.Store() {
            public int write(List<IgniteHeartbeatStore.Row> rows) throws Exception {
                entered.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
                committed.addAll(rows);
                return rows.size();
            }
            public void close() { }
        };
        try (AsyncHeartbeatWriter writer = new AsyncHeartbeatWriter(settings(), store)) {
            ElevatorUpdate update = IgniteStoreTest.update(1, 1, 1);
            update.state.speed = 7L;
            writer.submit(update);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            update.state.speed = 999L; update.elevatorId = 99;
            CompletableFuture<Void> snapshot = CompletableFuture.runAsync(() -> {
                try { writer.flush(); } catch (Exception e) { throw new RuntimeException(e); }
            });
            assertThrows(java.util.concurrent.TimeoutException.class, () -> snapshot.get(150, TimeUnit.MILLISECONDS));
            release.countDown();
            snapshot.get(5, TimeUnit.SECONDS);
            assertEquals(1, committed.get(0).elevatorId);
            assertEquals(7L, committed.get(0).values.get("Speed"));
        } finally { release.countDown(); }
    }

    @Test void fullQueueBackpressuresAndNeverDropsRecords() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        List<IgniteHeartbeatStore.Row> committed = new ArrayList<>();
        AsyncHeartbeatWriter.Store store = new AsyncHeartbeatWriter.Store() {
            public int write(List<IgniteHeartbeatStore.Row> rows) throws Exception {
                entered.countDown();
                release.await(); committed.addAll(rows); return rows.size();
            }
            public void close() { }
        };
        try (AsyncHeartbeatWriter writer = new AsyncHeartbeatWriter(settings(), store)) {
            writer.submit(IgniteStoreTest.update(1, 1, 1));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            writer.submit(IgniteStoreTest.update(1, 2, 2));
            CompletableFuture<Void> third = CompletableFuture.runAsync(() -> {
                try { writer.submit(IgniteStoreTest.update(1, 3, 3)); } catch (Exception e) { throw new RuntimeException(e); }
            });
            assertThrows(java.util.concurrent.TimeoutException.class, () -> third.get(150, TimeUnit.MILLISECONDS));
            assertEquals(1, writer.queued());
            release.countDown(); third.get(5, TimeUnit.SECONDS); writer.flush();
            assertEquals(3, committed.size());
            assertEquals(3, committed.get(2).createdTimeMs);
        } finally { release.countDown(); }
    }

    @Test void writeFailurePreventsCheckpointAcknowledgement() throws Exception {
        AsyncHeartbeatWriter.Store store = new AsyncHeartbeatWriter.Store() {
            public int write(List<IgniteHeartbeatStore.Row> rows) throws IOException { throw new IOException("injected database failure"); }
            public void close() { }
        };
        try (AsyncHeartbeatWriter writer = new AsyncHeartbeatWriter(settings(), store)) {
            // Worker may fail before submit returns; either boundary must propagate the failure.
            try { writer.submit(IgniteStoreTest.update(1, 1, 1)); } catch (IOException expected) { }
            IOException error = assertThrows(IOException.class, writer::flush);
            assertEquals("injected database failure", error.getCause().getMessage());
            assertEquals(0, writer.committed.get());
        }
    }
}
