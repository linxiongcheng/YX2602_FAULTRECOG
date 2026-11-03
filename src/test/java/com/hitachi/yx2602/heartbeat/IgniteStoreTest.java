package com.hitachi.yx2602.heartbeat;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Real Ignite 2.18.0 server, SQL provisioning, thin clients and durable storage. */
@Timeout(90)
class IgniteStoreTest {
    @TempDir static Path directory;
    private static Ignite server;
    private static IgniteConfig settings;

    @BeforeAll static void provision() throws Exception {
        Properties properties = ReaderConfigTest.properties();
        properties.setProperty("ignite.addresses", "127.0.0.1:10990");
        settings = ReaderConfig.from(properties, key -> null).ignite;
        startServer();
        String sql = new String(Files.readAllBytes(java.nio.file.Paths.get("sql/ignite2-heartbeat.sql")),
                java.nio.charset.StandardCharsets.UTF_8);
        sql = sql.replaceAll("(?m)^--.*$", "");
        for (String statement : sql.split(";")) {
            if (!statement.trim().isEmpty()) query(statement);
        }
    }

    private static void startServer() {
        DataRegionConfiguration region = new DataRegionConfiguration().setName(IgniteHeartbeatStore.Schema.REGION)
                .setPersistenceEnabled(true).setInitialSize(32L << 20).setMaxSize(128L << 20);
        IgniteConfiguration cfg = new IgniteConfiguration()
                .setIgniteInstanceName("heartbeat-integration-test").setConsistentId("heartbeat-test-node")
                .setLocalHost("127.0.0.1").setWorkDirectory(directory.resolve("work").toString())
                .setGridLogger(new NullLogger()).setMetricsLogFrequency(0)
                .setPublicThreadPoolSize(2).setSystemThreadPoolSize(2).setStripedPoolSize(2)
                .setQueryThreadPoolSize(2).setServiceThreadPoolSize(2).setAsyncCallbackPoolSize(2)
                .setDataStreamerThreadPoolSize(2).setRebalanceThreadPoolSize(1)
                .setClientConnectorConfiguration(new ClientConnectorConfiguration().setHost("127.0.0.1")
                        .setPort(10990).setPortRange(0).setThreadPoolSize(2))
                .setCommunicationSpi(new TcpCommunicationSpi().setLocalPort(47190).setSelectorsCount(1))
                .setDiscoverySpi(new TcpDiscoverySpi().setLocalPort(47590).setIpFinder(
                        new TcpDiscoveryVmIpFinder().setAddresses(Collections.singletonList("127.0.0.1:47590"))))
                .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(region)
                        .setStoragePath(directory.resolve("db").toString())
                        .setWalPath(directory.resolve("wal").toString())
                        .setWalArchivePath(directory.resolve("wal-archive").toString())
                        .setWalSegmentSize(8 << 20).setWalMode(WALMode.FSYNC));
        server = Ignition.start(cfg);
        server.cluster().state(ClusterState.ACTIVE);
    }

    @AfterEach void clearRows() {
        server.cache(IgniteHeartbeatStore.Schema.LATEST).clear();
        server.cache(IgniteHeartbeatStore.Schema.HISTORY).clear();
    }

    @AfterAll static void stopServer() { if (server != null) server.close(); }

    private static List<List<?>> query(String sql) {
        // A dedicated small cache is used solely to issue DDL/SQL through the public server API.
        try (org.apache.ignite.cache.query.FieldsQueryCursor<List<?>> cursor = server.getOrCreateCache("test-sql")
                .query(new SqlFieldsQuery(sql).setSchema("PUBLIC"))) {
            return cursor.getAll();
        }
    }

    static ElevatorUpdate update(long id, long time, long flinkSequence) {
        ElevatorUpdate u = new ElevatorUpdate();
        u.elevatorId = id; u.createdTimeMs = time; u.sendTimeMs = time; u.historySequence = flinkSequence;
        u.port = 3; u.sourceTopic = "heartbeat-test"; u.sourceOffset = time;
        u.sourcePartition = 1; u.kafkaTimestampMs = time; u.receivedAtMs = time; u.parsedAtMs = time;
        u.state = new ElevatorState();
        u.state.mainFaultCode = "0007";
        return u;
    }

    private static List<IgniteHeartbeatStore.Row> rows(ElevatorUpdate... updates) {
        List<IgniteHeartbeatStore.Row> rows = new ArrayList<>();
        for (ElevatorUpdate update : updates) rows.add(IgniteHeartbeatStore.Row.copyOf(update));
        return rows;
    }

    private static IgniteConfig dynamicConfig(Path file) {
        Properties p = ReaderConfigTest.properties();
        p.setProperty("ignite.addresses", settings.addresses);
        p.setProperty("ignite.history.config.path", file.toUri().toString());
        p.setProperty("ignite.history.refresh.interval.ms", "1000");
        return ReaderConfig.from(p, key -> null).ignite;
    }

    private static void policyFile(Path file, long version, int count) throws Exception {
        Path temporary = file.resolveSibling(file.getFileName() + ".next");
        Files.write(temporary, ("version=" + version + "\nmax.records=" + count + "\n")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Files.move(temporary, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    private static void awaitPolicy(IgniteHeartbeatStore store, long version) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (store.historyPolicy().version != version && System.nanoTime() < deadline) Thread.sleep(20);
        assertEquals(version, store.historyPolicy().version, "background refresh must apply without restarting writer");
    }

    private static void writeRange(IgniteHeartbeatStore store, long elevatorId, int from, int to) throws Exception {
        List<IgniteHeartbeatStore.Row> batch = new ArrayList<>();
        for (int sequence = from; sequence <= to; sequence++) {
            batch.add(IgniteHeartbeatStore.Row.copyOf(update(elevatorId, sequence, sequence)));
        }
        assertEquals(to - from + 1, store.write(batch));
    }

    private static void assertHistory(long elevatorId, int from, int to) {
        List<List<?>> actual = query("SELECT DB_SEQUENCE FROM HB_HISTORY WHERE ELEVATOR_ID=" + elevatorId + " ORDER BY DB_SEQUENCE");
        assertEquals(to - from + 1, actual.size());
        for (int sequence = from; sequence <= to; sequence++) assertEquals((long) sequence, actual.get(sequence - from).get(0));
    }

    @Test void hotReloadResizesRingAndLeavesOfflineElevatorsUntilTheirNextNewHeartbeat() throws Exception {
        Path file = directory.resolve("dynamic-history.properties"); policyFile(file, 1, 10);
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(dynamicConfig(file))) {
            writeRange(store, 77, 1, 12); writeRange(store, 88, 1, 12);
            assertHistory(77, 3, 12);
            policyFile(file, 2, 20); awaitPolicy(store, 2);
            writeRange(store, 77, 13, 22); assertHistory(77, 3, 22);
            policyFile(file, 3, 5); awaitPolicy(store, 3);
            assertEquals(0, store.write(rows(update(77, 22, 22))));
            assertHistory(77, 3, 22); // Duplicate data does not trigger lazy cleanup.
            writeRange(store, 77, 23, 23); assertHistory(77, 19, 23);
            assertHistory(88, 3, 12); // No new heartbeat yet.
            assertEquals(0L, query("SELECT COUNT(*) FROM HB_HISTORY WHERE ELEVATOR_ID=77 AND SLOT>=5").get(0).get(0));
            policyFile(file, 4, 1); awaitPolicy(store, 4);
            writeRange(store, 77, 24, 24); assertHistory(77, 24, 24);
            policyFile(file, 5, 4); awaitPolicy(store, 5);
            writeRange(store, 77, 25, 25); assertHistory(77, 24, 25); // Deleted data cannot be recreated.
            writeRange(store, 77, 26, 28); assertHistory(77, 25, 28);
            assertEquals(Arrays.asList(4, 5L), query("SELECT HISTORY_LIMIT,HISTORY_POLICY_VERSION FROM HB_LATEST WHERE ELEVATOR_ID=77").get(0));
            writeRange(store, 88, 13, 13); assertHistory(88, 10, 13);
        }
    }

    @Test void staleWorkerCannotRevertNewerDatabasePolicy() throws Exception {
        // Separate files model two tasks that have observed different revisions of the shared file.
        Path slowFile = directory.resolve("slow-policy.properties"), fastFile = directory.resolve("fast-policy.properties");
        policyFile(slowFile, 1, 10); policyFile(fastFile, 2, 5);
        try (IgniteHeartbeatStore slow = new IgniteHeartbeatStore(dynamicConfig(slowFile));
             IgniteHeartbeatStore fast = new IgniteHeartbeatStore(dynamicConfig(fastFile))) {
            writeRange(slow, 77, 1, 12);
            writeRange(fast, 77, 13, 13);
            writeRange(slow, 77, 14, 14);
            assertHistory(77, 10, 14);
            assertEquals(Arrays.asList(5, 2L), query("SELECT HISTORY_LIMIT,HISTORY_POLICY_VERSION FROM HB_LATEST WHERE ELEVATOR_ID=77").get(0));
        }
    }

    @Test void resizeAndPolicyMetadataRollBackWithFailedTransaction() throws Exception {
        Path file = directory.resolve("rollback-policy.properties"); policyFile(file, 1, 10);
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(dynamicConfig(file))) {
            writeRange(store, 1, 1, 12); writeRange(store, 2, 1, 1);
            org.apache.ignite.IgniteCache<Long, BinaryObject> cache = server.<Long, BinaryObject>cache(IgniteHeartbeatStore.Schema.LATEST).withKeepBinary();
            cache.put(2L, cache.get(2L).toBuilder().setField("DB_SEQUENCE", Long.MAX_VALUE).build());
            policyFile(file, 2, 2); awaitPolicy(store, 2);
            assertThrows(ArithmeticException.class, () -> store.write(rows(update(1, 13, 13), update(2, 2, 2))));
            assertHistory(1, 3, 12);
            assertEquals(Arrays.asList(12L, 10, 1L), query("SELECT DB_SEQUENCE,HISTORY_LIMIT,HISTORY_POLICY_VERSION FROM HB_LATEST WHERE ELEVATOR_ID=1").get(0));
            writeRange(store, 1, 13, 13); assertHistory(1, 12, 13);
        }
    }

    @Test void fixedConfigCanChooseNonDefaultHistorySize() throws Exception {
        Properties p = ReaderConfigTest.properties();
        p.setProperty("ignite.addresses", settings.addresses);
        p.setProperty("ignite.history.max.records", "3");
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(ReaderConfig.from(p, key -> null).ignite)) {
            writeRange(store, 77, 1, 8); assertHistory(77, 6, 8);
        }
    }

    @Test void sqlSchemaNullsRingAndReplayUseDatabaseSequence() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            List<IgniteHeartbeatStore.Row> input = new ArrayList<>();
            for (int n = 1; n <= 12; n++) {
                ElevatorUpdate u = update(77, n, n + 900);
                if (n < 12) u.state.speed = 100L;
                input.add(IgniteHeartbeatStore.Row.copyOf(u));
            }
            assertEquals(12, store.write(input));
            // A restored Flink checkpoint has lower sequence values and replays already committed input.
            assertEquals(0, store.write(rows(update(77, 12, 3), update(77, 8, 4))));
            assertEquals(Arrays.asList(12L, 12L, 912L, "0007", null),
                    query("SELECT CREATED_TIME_MS, DB_SEQUENCE, FLINK_SEQUENCE, STATE_MAIN_FAULT_CODE, STATE_SPEED "
                            + "FROM HB_LATEST WHERE ELEVATOR_ID=77").get(0));
            List<List<?>> history = query("SELECT DB_SEQUENCE FROM HB_HISTORY WHERE ELEVATOR_ID=77 ORDER BY DB_SEQUENCE");
            assertEquals(10, history.size());
            assertEquals(3L, history.get(0).get(0));
            assertEquals(12L, history.get(9).get(0));
            assertEquals(1, store.write(rows(update(77, 13, 5))));
            assertEquals(Arrays.asList(13L, 5L), query("SELECT DB_SEQUENCE, FLINK_SEQUENCE FROM HB_LATEST").get(0));
            assertEquals(10L, query("SELECT COUNT(*) FROM HB_HISTORY").get(0).get(0));
        }
    }

    @Test void exceptionRollsBackLatestHistoryAndSequenceAcrossEntireBatch() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            store.write(rows(update(2, 1, 1)));
            // Force a real mid-transaction failure after elevator 1 has been staged.
            org.apache.ignite.IgniteCache<Long, BinaryObject> cache = server.<Long, BinaryObject>cache(IgniteHeartbeatStore.Schema.LATEST).withKeepBinary();
            cache.put(2L, cache.get(2L).toBuilder().setField("DB_SEQUENCE", Long.MAX_VALUE).build());
            assertThrows(ArithmeticException.class, () -> store.write(rows(update(1, 2, 1), update(2, 2, 2))));
            assertEquals(0L, query("SELECT COUNT(*) FROM HB_LATEST WHERE ELEVATOR_ID=1").get(0).get(0));
            assertEquals(0L, query("SELECT COUNT(*) FROM HB_HISTORY WHERE ELEVATOR_ID=1").get(0).get(0));
            assertEquals(1L, query("SELECT CREATED_TIME_MS FROM HB_LATEST WHERE ELEVATOR_ID=2").get(0).get(0));
            assertEquals(1L, query("SELECT MAX(DB_SEQUENCE) FROM HB_HISTORY WHERE ELEVATOR_ID=2").get(0).get(0));
        }
    }

    @Test void databaseOrdersByCreatedDateEvenIfSendTimeIsMissingOrMovesBackwards() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            ElevatorUpdate first = update(66, 100, 1);
            first.sendTimeMs = 9000L;
            assertEquals(1, store.write(rows(first)));
            ElevatorUpdate second = update(66, 200, 2);
            second.sendTimeMs = null; second.sendTimeRaw = "\"\"";
            assertEquals(1, store.write(rows(second)));
            ElevatorUpdate stale = update(66, 150, 3);
            stale.sendTimeMs = Long.MAX_VALUE;
            assertEquals(0, store.write(rows(stale)));
            ElevatorUpdate duplicate = update(66, 200, 4);
            duplicate.sendTimeMs = Long.MAX_VALUE;
            assertEquals(0, store.write(rows(duplicate)));
            assertEquals(Arrays.asList(200L, null, "\"\"", 2L), query(
                    "SELECT CREATED_TIME_MS, SEND_TIME_MS, SEND_TIME_RAW, DB_SEQUENCE FROM HB_LATEST WHERE ELEVATOR_ID=66").get(0));
            assertEquals(2L, query("SELECT COUNT(*) FROM HB_HISTORY WHERE ELEVATOR_ID=66").get(0).get(0));
        }
    }

    @Test void concurrentFirstWritersCannotRegressOrReuseSequence() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int n = 1; n <= 2; n++) {
            final int time = n;
            futures.add(CompletableFuture.runAsync(() -> {
                try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
                    barrier.await(10, TimeUnit.SECONDS);
                    store.write(rows(update(88, time, time)));
                } catch (Exception e) { throw new RuntimeException(e); }
            }));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
        List<?> latest = query("SELECT CREATED_TIME_MS, DB_SEQUENCE FROM HB_LATEST WHERE ELEVATOR_ID=88").get(0);
        assertEquals(2L, latest.get(0));
        assertEquals(latest.get(1), query("SELECT COUNT(*) FROM HB_HISTORY WHERE ELEVATOR_ID=88").get(0).get(0));
        assertEquals(latest.get(1), query("SELECT MAX(DB_SEQUENCE) FROM HB_HISTORY WHERE ELEVATOR_ID=88").get(0).get(0));
    }

    @Test void durableRestartKeepsBothTablesAndExistingClientCanReconnect() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            store.write(rows(update(99, 1, 1)));
            server.close();
            startServer();
            assertEquals(1L, query("SELECT CREATED_TIME_MS FROM HB_LATEST WHERE ELEVATOR_ID=99").get(0).get(0));
            assertEquals(1L, query("SELECT COUNT(*) FROM HB_HISTORY WHERE ELEVATOR_ID=99").get(0).get(0));
            assertEquals(0, store.write(rows(update(99, 1, 1))));
            assertEquals(1, store.write(rows(update(99, 2, 2))));
            assertEquals(2L, query("SELECT DB_SEQUENCE FROM HB_LATEST WHERE ELEVATOR_ID=99").get(0).get(0));
        }
    }

    @Test void startupRejectsAtomicCacheAndMissingFields() {
        ClientCacheConfiguration config;
        try (org.apache.ignite.client.IgniteClient client = Ignition.startClient(
                new org.apache.ignite.configuration.ClientConfiguration().setAddresses(settings.addresses))) {
            config = client.cache(IgniteHeartbeatStore.Schema.LATEST).getConfiguration();
        }
        config.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        assertThrows(IllegalStateException.class, () -> IgniteHeartbeatStore.Schema.validate(config, IgniteHeartbeatStore.Schema.LATEST));
        config.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        config.getQueryEntities()[0].getFields().remove("STATE_SPEED");
        assertThrows(IllegalStateException.class, () -> IgniteHeartbeatStore.Schema.validate(config, IgniteHeartbeatStore.Schema.LATEST));
    }

    @Test void realFlinkSinkFlushesShortFinalBatch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        IgniteHeartbeatSink.attach(env.fromCollection(Arrays.asList(update(11, 1, 1), update(11, 2, 2), update(33, 1, 1))), settings);
        env.execute("heartbeat-ignite-integration");
        assertEquals(2L, query("SELECT COUNT(*) FROM HB_LATEST").get(0).get(0));
        assertEquals(3L, query("SELECT COUNT(*) FROM HB_HISTORY").get(0).get(0));
    }

    @Test void databaseOutageFailsCheckpointAndRestoredSinkReplaysSafely() throws Exception {
        Properties properties = ReaderConfigTest.properties();
        properties.setProperty("ignite.addresses", settings.addresses);
        properties.setProperty("ignite.request.timeout.ms", "500");
        properties.setProperty("ignite.transaction.timeout.ms", "500");
        properties.setProperty("ignite.retry.attempts", "2");
        properties.setProperty("ignite.retry.delay.ms", "50");
        properties.setProperty("ignite.flush.timeout.ms", "3000");
        IgniteConfig shortTimeout = ReaderConfig.from(properties, key -> null).ignite;
        org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
        try (org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<ElevatorUpdate, Object> harness =
                     sinkHarness(shortTimeout)) {
            harness.open();
            harness.processElement(new org.apache.flink.streaming.runtime.streamrecord.StreamRecord<>(update(44, 1, 1)));
            snapshot = harness.snapshot(1, 1);
            assertEquals(1L, query("SELECT COUNT(*) FROM HB_HISTORY WHERE ELEVATOR_ID=44").get(0).get(0));
            server.close();
            assertThrows(Exception.class, () -> {
                harness.processElement(new org.apache.flink.streaming.runtime.streamrecord.StreamRecord<>(update(44, 2, 2)));
                harness.snapshot(2, 2);
            });
        } finally { startServer(); }
        try (org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<ElevatorUpdate, Object> restored =
                     sinkHarness(shortTimeout)) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement(new org.apache.flink.streaming.runtime.streamrecord.StreamRecord<>(update(44, 1, 1)));
            restored.processElement(new org.apache.flink.streaming.runtime.streamrecord.StreamRecord<>(update(44, 2, 2)));
            restored.snapshot(3, 3);
            assertEquals(Arrays.asList(2L, 2L), query("SELECT CREATED_TIME_MS, DB_SEQUENCE FROM HB_LATEST WHERE ELEVATOR_ID=44").get(0));
            assertEquals(2L, query("SELECT COUNT(*) FROM HB_HISTORY WHERE ELEVATOR_ID=44").get(0).get(0));
        }
    }

    private org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<ElevatorUpdate, Object>
            sinkHarness(IgniteConfig config) throws Exception {
        return new org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness<>(
                new org.apache.flink.streaming.api.operators.StreamSink<ElevatorUpdate>(new IgniteHeartbeatSink(config)) {
                    // Harness.close normally finishes bounded input. A failed task is cancelled instead;
                    // it must close resources without attempting a successful end-of-input flush.
                    @Override public void finish() { }
                });
    }
}
