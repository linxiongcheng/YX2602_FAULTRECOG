package com.hitachi.yx2602.heartbeat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** FIFO barriers connect database commit completion to Flink checkpoint acknowledgement. */
final class AsyncHeartbeatWriter implements AutoCloseable {
    private final IgniteConfig config;
    private final HeartbeatStore store;
    private final ArrayBlockingQueue<Object> queue;
    private final Thread worker;
    private volatile Throwable failure;
    private volatile boolean stopped;
    final AtomicLong committed = new AtomicLong(), skipped = new AtomicLong();

    AsyncHeartbeatWriter(IgniteConfig config, HeartbeatStore store) {
        this.config = config;
        this.store = store;
        queue = new ArrayBlockingQueue<>(config.queueCapacity);
        worker = new Thread(this::run, "heartbeat-ignite-writer");
        worker.setDaemon(true);
        worker.start();
    }

    void submit(ElevatorUpdate update) throws Exception {
        enqueue(HeartbeatRow.copyOf(update), deadline());
    }

    void flush() throws Exception {
        long deadline = deadline();
        CountDownLatch barrier = new CountDownLatch(1);
        enqueue(barrier, deadline);
        while (true) {
            checkHealthy();
            if (barrier.await(100, TimeUnit.MILLISECONDS)) { checkHealthy(); return; }
            checkDeadline(deadline);
        }
    }

    int queued() { return queue.size(); }

    private void enqueue(Object entry, long deadline) throws Exception {
        while (true) {
            checkHealthy();
            if (queue.offer(entry, 100, TimeUnit.MILLISECONDS)) { checkHealthy(); return; }
            checkDeadline(deadline);
        }
    }

    private void run() {
        try {
            while (!stopped) {
                Object first = queue.take();
                if (first instanceof CountDownLatch) { ((CountDownLatch) first).countDown(); continue; }
                List<HeartbeatRow> batch = new ArrayList<>(config.batchSize);
                batch.add((HeartbeatRow) first);
                CountDownLatch barrier = null;
                long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.lingerMs);
                while (batch.size() < config.batchSize) {
                    long remaining = until - System.nanoTime();
                    if (remaining <= 0) break;
                    Object next = queue.poll(remaining, TimeUnit.NANOSECONDS);
                    if (next == null) break;
                    if (next instanceof CountDownLatch) { barrier = (CountDownLatch) next; break; }
                    batch.add((HeartbeatRow) next);
                }
                if (stopped) break;
                int accepted = store.write(batch);
                committed.addAndGet(accepted);
                skipped.addAndGet(batch.size() - accepted);
                if (barrier != null) barrier.countDown();
            }
        } catch (Throwable error) {
            failure = error;
        } finally {
            try { store.close(); }
            catch (Throwable error) { if (failure == null) failure = error; }
        }
    }

    private long deadline() { return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.flushTimeoutMs); }

    private void checkDeadline(long deadline) throws IOException {
        if (System.nanoTime() >= deadline) throw new IOException("Ignite写入等待超时，触发Flink恢复");
    }

    private void checkHealthy() throws IOException {
        if (failure != null) throw new IOException("Ignite异步写入失败，触发Flink恢复", failure);
        if (stopped) throw new IOException("Ignite writer is closed");
    }

    @Override public void close() throws InterruptedException {
        // Cancellation must not flush records belonging to a failed task attempt.
        stopped = true;
        worker.interrupt();
        worker.join(config.requestTimeoutMs + (long) config.transactionTimeoutMs + 1000);
        if (worker.isAlive()) throw new IllegalStateException("Ignite writer未在超时内停止");
        queue.clear();
    }
}
