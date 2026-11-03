package com.hitachi.yx2602.heartbeat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** 有界队列异步批量写库，通过先进先出的屏障确保数据库提交完成后才确认 Flink 检查点。 */
final class AsyncHeartbeatWriter implements AutoCloseable {
    private final IgniteConfig config;
    private final Store store;
    private final ArrayBlockingQueue<Object> queue;
    private final Thread worker;
    private volatile Throwable failure;
    private volatile boolean stopped;
    final AtomicLong committed = new AtomicLong(), skipped = new AtomicLong();

    AsyncHeartbeatWriter(IgniteConfig config, Store store) {
        this.config = config;
        this.store = store;
        queue = new ArrayBlockingQueue<>(config.queueCapacity);
        worker = new Thread(this::run, "heartbeat-ignite-writer");
        worker.setDaemon(true);
        worker.start();
    }

    void submit(ElevatorUpdate update) throws Exception {
        // 入队前复制字段，避免 Flink 复用或后续修改原对象影响后台线程写入。
        enqueue(IgniteHeartbeatStore.Row.copyOf(update), deadline());
    }

    void flush() throws Exception {
        long deadline = deadline();
        // 屏障与数据共用队列；后台线程只有提交完屏障之前的数据，才解除等待。
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
        // 队列满时阻塞上游形成背压，并持续检查后台异常和超时，避免无限积压。
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
                List<IgniteHeartbeatStore.Row> batch = new ArrayList<>(config.batchSize);
                batch.add((IgniteHeartbeatStore.Row) first);
                CountDownLatch barrier = null;
                long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.lingerMs);
                while (batch.size() < config.batchSize) {
                    long remaining = until - System.nanoTime();
                    if (remaining <= 0) break;
                    Object next = queue.poll(remaining, TimeUnit.NANOSECONDS);
                    if (next == null) break;
                    if (next instanceof CountDownLatch) { barrier = (CountDownLatch) next; break; }
                    batch.add((IgniteHeartbeatStore.Row) next);
                }
                if (stopped) break;
                // write 返回代表事务已提交；若失败则由 failure 传回 Flink，不能提前确认屏障。
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
        // 取消时不主动刷出剩余队列；正常结束由 finish 刷出，失败恢复依靠重放和数据库幂等判断。
        stopped = true;
        worker.interrupt();
        worker.join(config.requestTimeoutMs + (long) config.transactionTimeoutMs + 1000);
        if (worker.isAlive()) throw new IllegalStateException("Ignite writer未在超时内停止");
        queue.clear();
    }

    /** 一批数据必须等数据库事务提交后才能返回成功，返回值为实际接受的记录数。 */
    interface Store extends AutoCloseable {
        int write(List<IgniteHeartbeatStore.Row> rows) throws Exception;
        @Override void close();
    }
}
