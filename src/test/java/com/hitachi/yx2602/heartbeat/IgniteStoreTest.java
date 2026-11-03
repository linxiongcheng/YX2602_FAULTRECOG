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
        DataRegionConfiguration region = new DataRegionConfiguration().setName(IgniteSchema.REGION)
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
        server.cache(IgniteSchema.LATEST).clear();
        server.cache(IgniteSchema.HISTORY).clear();
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
        u.elevatorId = id; u.sendTimeMs = time; u.historySequence = flinkSequence;
        u.port = 3; u.sourceTopic = "heartbeat-test"; u.sourceOffset = time;
        u.sourcePartition = 1; u.kafkaTimestampMs = time; u.receivedAtMs = time; u.parsedAtMs = time;
        u.state = new ElevatorState();
        u.state.mainFaultCode = "0007";
        return u;
    }

    private static List<HeartbeatRow> rows(ElevatorUpdate... updates) {
        List<HeartbeatRow> rows = new ArrayList<>();
        for (ElevatorUpdate update : updates) rows.add(HeartbeatRow.copyOf(update));
        return rows;
    }

    @Test void sqlSchemaNullsRingAndReplayUseDatabaseSequence() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            List<HeartbeatRow> input = new ArrayList<>();
            for (int n = 1; n <= 12; n++) {
                ElevatorUpdate u = update(77, n, n + 900);
                if (n < 12) u.state.speed = 100L;
                input.add(HeartbeatRow.copyOf(u));
            }
            assertEquals(12, store.write(input));
            // A restored Flink checkpoint has lower sequence values and replays already committed input.
            assertEquals(0, store.write(rows(update(77, 12, 3), update(77, 8, 4))));
            assertEquals(Arrays.asList(12L, 12L, 912L, "0007", null),
                    query("SELECT SEND_TIME_MS, DB_SEQUENCE, FLINK_SEQUENCE, STATE_MAIN_FAULT_CODE, STATE_SPEED "
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
            org.apache.ignite.IgniteCache<Long, BinaryObject> cache = server.<Long, BinaryObject>cache(IgniteSchema.LATEST).withKeepBinary();
            cache.put(2L, cache.get(2L).toBuilder().setField("DB_SEQUENCE", Long.MAX_VALUE).build());
            assertThrows(ArithmeticException.class, () -> store.write(rows(update(1, 2, 1), update(2, 2, 2))));
            assertEquals(0L, query("SELECT COUNT(*) FROM HB_LATEST WHERE ELEVATOR_ID=1").get(0).get(0));
            assertEquals(0L, query("SELECT COUNT(*) FROM HB_HISTORY WHERE ELEVATOR_ID=1").get(0).get(0));
            assertEquals(1L, query("SELECT SEND_TIME_MS FROM HB_LATEST WHERE ELEVATOR_ID=2").get(0).get(0));
            assertEquals(1L, query("SELECT MAX(DB_SEQUENCE) FROM HB_HISTORY WHERE ELEVATOR_ID=2").get(0).get(0));
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
        List<?> latest = query("SELECT SEND_TIME_MS, DB_SEQUENCE FROM HB_LATEST WHERE ELEVATOR_ID=88").get(0);
        assertEquals(2L, latest.get(0));
        assertEquals(latest.get(1), query("SELECT COUNT(*) FROM HB_HISTORY WHERE ELEVATOR_ID=88").get(0).get(0));
        assertEquals(latest.get(1), query("SELECT MAX(DB_SEQUENCE) FROM HB_HISTORY WHERE ELEVATOR_ID=88").get(0).get(0));
    }

    @Test void durableRestartKeepsBothTablesAndExistingClientCanReconnect() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            store.write(rows(update(99, 1, 1)));
            server.close();
            startServer();
            assertEquals(1L, query("SELECT SEND_TIME_MS FROM HB_LATEST WHERE ELEVATOR_ID=99").get(0).get(0));
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
            config = client.cache(IgniteSchema.LATEST).getConfiguration();
        }
        config.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        assertThrows(IllegalStateException.class, () -> IgniteSchema.validate(config, IgniteSchema.LATEST));
        config.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        config.getQueryEntities()[0].getFields().remove("STATE_SPEED");
        assertThrows(IllegalStateException.class, () -> IgniteSchema.validate(config, IgniteSchema.LATEST));
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
            assertEquals(Arrays.asList(2L, 2L), query("SELECT SEND_TIME_MS, DB_SEQUENCE FROM HB_LATEST WHERE ELEVATOR_ID=44").get(0));
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
