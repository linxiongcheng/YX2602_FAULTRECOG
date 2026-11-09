package com.hitachi.yx2602.heartbeat;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

/** Real Ignite 2.18.0 in-memory server, cold table creation and transactional thin clients. */
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
        try (IgniteHeartbeatStore ignored = new IgniteHeartbeatStore(settings)) { }
    }

    private static void startServer() {
        DataRegionConfiguration region = new DataRegionConfiguration().setName("Memory_Region")
                .setPersistenceEnabled(false).setInitialSize(32L << 20).setMaxSize(128L << 20)
                .setPageEvictionMode(org.apache.ignite.configuration.DataPageEvictionMode.RANDOM_2_LRU)
                .setEvictionThreshold(0.7);
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
                .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(region));
        server = Ignition.start(cfg);
        server.cluster().state(ClusterState.ACTIVE);
    }

    @AfterEach void clearRows() {
        server.cache(IgniteHeartbeatStore.Schema.CACHE).clear();
    }

    @AfterAll static void stopServer() { if (server != null) server.close(); }

    private static List<List<?>> query(String sql, Object... args) {
        // A dedicated small cache is used solely to issue DDL/SQL through the public server API.
        try (org.apache.ignite.cache.query.FieldsQueryCursor<List<?>> cursor = server.getOrCreateCache("test-sql")
                .query(new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(args))) {
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
        List<List<?>> actual = query("SELECT DB_SEQUENCE FROM HB_STATE WHERE SLOT>=0 AND eid=" + elevatorId + " ORDER BY DB_SEQUENCE");
        assertEquals(to - from + 1, actual.size());
        for (int sequence = from; sequence <= to; sequence++) assertEquals((long) sequence, actual.get(sequence - from).get(0));
        // 合并后总行数为N+1，历史扩缩容不能删除或额外复制-1固定槽位。
        assertEquals((long) actual.size() + 1, query("SELECT COUNT(*) FROM HB_STATE WHERE eid=" + elevatorId).get(0).get(0));
        String columns = "DB_SEQUENCE, CREATED_TIME_MS, HISTORY_LIMIT, HISTORY_POLICY_VERSION";
        List<List<?>> latest = query("SELECT " + columns + " FROM HB_STATE WHERE SLOT=-1 AND eid=" + elevatorId);
        assertEquals(1, latest.size());
        assertEquals((long) to, latest.get(0).get(0));
        assertEquals(latest, query("SELECT " + columns + " FROM HB_STATE WHERE SLOT>=0 AND eid="
                + elevatorId + " AND DB_SEQUENCE=" + to));
    }

    /** Count calls at the actual thin-client boundary while still using the real server. */
    private static final class CacheProbe {
        final java.util.Map<String, Integer> calls = new java.util.LinkedHashMap<>();
        final List<Long> latestReads = new ArrayList<>();
        boolean failAfterBulkWrite;
        java.util.function.Consumer<String> beforeCall = name -> { };

        CacheProbe(IgniteHeartbeatStore store) throws Exception {
            java.lang.reflect.Field cache = IgniteHeartbeatStore.class.getDeclaredField("states");
            cache.setAccessible(true);
            Object delegate = cache.get(store);
            Object proxy = java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.ignite.client.ClientCache.class.getClassLoader(),
                    new Class<?>[]{org.apache.ignite.client.ClientCache.class}, (ignored, method, args) -> {
                        calls.merge(method.getName(), 1, Integer::sum);
                        beforeCall.accept(method.getName());
                        if (method.getName().equals("get")) {
                            org.apache.ignite.binary.BinaryObject key = (org.apache.ignite.binary.BinaryObject) args[0];
                            if (Integer.valueOf(-1).equals(key.field("SLOT"))) latestReads.add(key.field("EID"));
                        }
                        Object result;
                        try { result = method.invoke(delegate, args); }
                        catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                        if (failAfterBulkWrite && method.getName().equals("putAll")) {
                            throw new IllegalStateException("injected failure after real bulk writes");
                        }
                        return result;
                    });
            cache.set(store, proxy);
        }

        int count(String name) { return calls.getOrDefault(name, 0); }
    }

    @Test void diagnosticReportIdentifiesBlockedBulkCallBeforeTransactionCanCommit() throws Exception {
        java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong();
        List<String> logs = new ArrayList<>();
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        try (HeartbeatDiagnostics monitor = new HeartbeatDiagnostics("ignite_sink", "subtask=0/1 attempt=0",
                1000, clock::get, logs::add);
             IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings, monitor)) {
            CacheProbe probe = new CacheProbe(store);
            probe.beforeCall = name -> {
                if (!name.equals("putAll")) return;
                entered.countDown();
                try { assertTrue(release.await(5, TimeUnit.SECONDS)); }
                catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new RuntimeException(error); }
            };
            CompletableFuture<Integer> writing = CompletableFuture.supplyAsync(() -> {
                try { return store.write(rows(update(77, 1, 1))); }
                catch (Exception error) { throw new RuntimeException(error); }
            });
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                clock.set(TimeUnit.MILLISECONDS.toNanos(500));
                String blocked = monitor.snapshot();
                assertTrue(blocked.contains("latest_read_samples=1"), blocked);
                assertTrue(blocked.contains("bulk_write_active_ms=500"), blocked);
                assertFalse(blocked.contains("commit_samples="), blocked);
            } finally { release.countDown(); }
            assertEquals(1, writing.get(5, TimeUnit.SECONDS));
            clock.set(TimeUnit.SECONDS.toNanos(1));
            String finished = monitor.snapshot();
            assertTrue(finished.contains("bulk_write_samples=1"), finished);
            assertTrue(finished.contains("commit_samples=1"), finished);
            assertTrue(finished.contains("bulk_write_active_ms=0"), finished);
        } finally { release.countDown(); }
    }

    @Test void distinctElevatorBatchUsesOrderedReadsAndOneBulkWrite() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            CacheProbe probe = new CacheProbe(store);
            List<IgniteHeartbeatStore.Row> input = new ArrayList<>();
            for (int id = 128; id >= 1; id--) input.addAll(rows(update(id, 1, 1)));
            assertEquals(128, store.write(input));
            System.out.println("128-elevator thin-client calls: " + probe.calls);
            assertEquals(128, probe.count("get"));
            assertEquals(1, probe.count("putAll"));
            assertEquals(0, probe.count("put"));
            for (int id = 1; id <= 128; id++) assertEquals((long) id, probe.latestReads.get(id - 1));
            assertEquals(256L, query("SELECT COUNT(*) FROM HB_STATE").get(0).get(0));
        }
    }

    @Test void repeatedElevatorBatchReadsOnceAndPreservesEveryAcceptedSequence() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            CacheProbe probe = new CacheProbe(store);
            List<IgniteHeartbeatStore.Row> input = rows(update(77, 1, 1), update(77, 1, 2));
            for (int n = 2; n <= 128; n++) {
                input.addAll(rows(update(77, n, n + 10), update(77, n - 1, n + 20)));
            }
            assertEquals(128, store.write(input));
            System.out.println("One-elevator thin-client calls: " + probe.calls);
            assertEquals(1, probe.count("get"));
            assertEquals(1, probe.count("putAll"));
            assertEquals(0, probe.count("put"));
            assertHistory(77, 119, 128);
            probe.calls.clear();
            assertEquals(0, store.write(rows(update(77, 128, 1), update(77, 127, 2))));
            assertEquals(1, probe.count("get"));
            assertEquals(0, probe.count("putAll"));
        }
    }

    @Test void bulkWriteFailureRollsBackChangesAlreadySentToServer() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            store.write(rows(update(1, 1, 1), update(2, 1, 1)));
            CacheProbe probe = new CacheProbe(store);
            probe.failAfterBulkWrite = true;
            assertThrows(IllegalStateException.class,
                    () -> store.write(rows(update(1, 2, 2), update(2, 2, 2))));
            assertEquals(1, probe.count("putAll"));
            assertHistory(1, 1, 1);
            assertHistory(2, 1, 1);
        }
    }

    @Test void resizedBatchKeepsNewestHistoryWithDuplicatesAndMoreUpdatesThanSlots() throws Exception {
        Path file = directory.resolve("batch-history.properties"); policyFile(file, 1, 10);
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(dynamicConfig(file))) {
            writeRange(store, 77, 1, 10);
            policyFile(file, 2, 3); awaitPolicy(store, 2);
            List<IgniteHeartbeatStore.Row> input = rows(update(77, 10, 20), update(77, 11, 21), update(77, 10, 22));
            for (int n = 12; n <= 18; n++) input.addAll(rows(update(77, n, n + 20)));
            assertEquals(8, store.write(input));
            assertHistory(77, 16, 18);
            policyFile(file, 3, 6); awaitPolicy(store, 3);
            writeRange(store, 77, 19, 21);
            assertHistory(77, 16, 21);
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 3})
    void documentFieldsRoundTripFromEachPortToBothSlotKinds(int port) throws Exception {
        // 独立按文档的拆分后41个字段构造输入/期望值，验证真实解析到SQL存储的字段对应关系。
        java.util.Map<String, Object> expected = new java.util.LinkedHashMap<>();
        expected.put("EId", 773282L); expected.put("IntId", 3729739L); expected.put("FId", 42L);
        expected.put("Code", "001234567890");
        String[] portNames = {"Floor", "ACD", "Direction", "RS", "DoorLock", "IsActive", "IsDebug",
                "IsRepair", "IsPower", "IsDoor", "IsHardware", "IsCall", "RunTotal", "RunTime", "Speed",
                "DoorCounter", "SecDoorCouter", "Weight", "TCDA", "TCDB", "TCDC", "TCDStopOutsideDoor",
                "TCDOpenDoorRuning", "TCDEntrapment", "TCDDoor"};
        long[] values = {-2, 39, 2, 0, 1, 1, 0, 1, 0, 1, 0, 1, 715346, 225641, 98,
                456, 123, 65, 1, 0, 1, 0, 1, 0, 1};
        for (int i = 0; i < portNames.length; i++) expected.put(portNames[i], values[i]);
        expected.put("MainFaultCode", "0007"); expected.put("SubFaultCode", "0002");
        expected.put("SendCount", 1422L); expected.put("ReceiveCount", 1400L);
        expected.put("SignalRssi", 26L); expected.put("NetworkType", 2L);
        // 北京时间刚跨日，UTC仍在前一天；part_day必须跟随CreatedDate的北京时间。
        long createdTime = HeartbeatTime.parseCreatedDate("2026/1/23 0:00:00");
        expected.put("SendTime", createdTime + 5000);
        expected.put("RecordStatus", 1L); expected.put("Status", 0L);
        expected.put("CreatedDate", "2026/1/23 0:00:00");
        expected.put("UpdatedDate", "2026/1/23 0:00:08");
        expected.put("part_day", "2026-01-23");
        assertEquals(41, expected.size());
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.node.ObjectNode input = mapper.createObjectNode();
        java.util.Set<String> perPort = new java.util.HashSet<>(Arrays.asList(portNames));
        perPort.addAll(Arrays.asList("EId", "MainFaultCode", "SubFaultCode"));
        expected.forEach((name, value) -> {
            if (!name.equals("part_day")) input.set(perPort.contains(name) ? name + port : name, mapper.valueToTree(value));
        });
        RawHeartbeat raw = new RawHeartbeat(mapper.writeValueAsBytes(input), null, "document-fixture", 0, 42, createdTime);
        HeartbeatValidator.Result validated = new HeartbeatValidator(60000).validate(raw, createdTime);
        assertNotNull(validated.valid, validated.errorJson);
        HeartbeatSplitter.Result parsed = new HeartbeatSplitter().split(validated.valid, createdTime);
        assertTrue(parsed.errors.isEmpty(), parsed.errors.toString());
        assertEquals(1, parsed.valid.size());
        ElevatorUpdate update = parsed.valid.get(0); update.historySequence = 1;
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            assertEquals(1, store.write(rows(update)));
        }
        String columns = String.join(",", expected.keySet());
        expected.put("CreatedDate", new java.sql.Timestamp(createdTime));
        expected.put("SendTime", new java.sql.Timestamp(createdTime + 5000));
        expected.put("UpdatedDate", new java.sql.Timestamp(HeartbeatTime.parseUpdatedDate("2026/1/23 0:00:08")));
        String mixedCaseSql = "SELECT " + columns + " FROM HB_STATE WHERE EId=773282 ORDER BY SLOT";
        List<List<?>> expectedRows = Arrays.asList(new ArrayList<>(expected.values()), new ArrayList<>(expected.values()));
        // All 41 fields must be readable without quotes in mixed, lower and upper case.
        assertEquals(expectedRows, query(mixedCaseSql));
        assertEquals(expectedRows, query(mixedCaseSql.toLowerCase(java.util.Locale.ROOT)));
        assertEquals(expectedRows, query(mixedCaseSql.toUpperCase(java.util.Locale.ROOT)));
        assertEquals(Arrays.asList(Arrays.asList(-1, port), Arrays.asList(0, port)),
                query("SELECT SLOT, PORT FROM HB_STATE WHERE eid=773282 ORDER BY SLOT"));
        assertFalse(server.cacheNames().contains("HB_LATEST"));
        assertFalse(server.cacheNames().contains("HB_HISTORY"));
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
            assertEquals(0L, query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT>=0 AND eid=77 AND SLOT>=5").get(0).get(0));
            policyFile(file, 4, 1); awaitPolicy(store, 4);
            writeRange(store, 77, 24, 24); assertHistory(77, 24, 24);
            policyFile(file, 5, 4); awaitPolicy(store, 5);
            writeRange(store, 77, 25, 25); assertHistory(77, 24, 25); // Deleted data cannot be recreated.
            writeRange(store, 77, 26, 28); assertHistory(77, 25, 28);
            assertEquals(Arrays.asList(4, 5L), query("SELECT HISTORY_LIMIT,HISTORY_POLICY_VERSION FROM HB_STATE WHERE SLOT=-1 AND eid=77").get(0));
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
            assertEquals(Arrays.asList(5, 2L), query("SELECT HISTORY_LIMIT,HISTORY_POLICY_VERSION FROM HB_STATE WHERE SLOT=-1 AND eid=77").get(0));
        }
    }

    @Test void resizeAndPolicyMetadataRollBackWithFailedTransaction() throws Exception {
        Path file = directory.resolve("rollback-policy.properties"); policyFile(file, 1, 10);
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(dynamicConfig(file))) {
            writeRange(store, 1, 1, 12); writeRange(store, 2, 1, 1);
            query("UPDATE HB_STATE SET DB_SEQUENCE=" + Long.MAX_VALUE + " WHERE eid=2 AND SLOT=-1");
            policyFile(file, 2, 2); awaitPolicy(store, 2);
            assertThrows(ArithmeticException.class, () -> store.write(rows(update(1, 13, 13), update(2, 2, 2))));
            assertHistory(1, 3, 12);
            assertEquals(Arrays.asList(12L, 10, 1L), query("SELECT DB_SEQUENCE,HISTORY_LIMIT,HISTORY_POLICY_VERSION FROM HB_STATE WHERE SLOT=-1 AND eid=1").get(0));
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
                    query("SELECT CREATED_TIME_MS, DB_SEQUENCE, FLINK_SEQUENCE, mainfaultcode, speed "
                            + "FROM HB_STATE WHERE SLOT=-1 AND eid=77").get(0));
            List<List<?>> history = query("SELECT DB_SEQUENCE FROM HB_STATE WHERE SLOT>=0 AND eid=77 ORDER BY DB_SEQUENCE");
            assertEquals(10, history.size());
            assertEquals(3L, history.get(0).get(0));
            assertEquals(12L, history.get(9).get(0));
            assertEquals(1, store.write(rows(update(77, 13, 5))));
            assertEquals(Arrays.asList(13L, 5L), query("SELECT DB_SEQUENCE, FLINK_SEQUENCE FROM HB_STATE WHERE SLOT=-1").get(0));
            assertEquals(10L, query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT>=0").get(0).get(0));
        }
    }

    @Test void exceptionRollsBackLatestHistoryAndSequenceAcrossEntireBatch() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            store.write(rows(update(2, 1, 1)));
            // Force a real mid-transaction failure after elevator 1 has been staged.
            query("UPDATE HB_STATE SET DB_SEQUENCE=" + Long.MAX_VALUE + " WHERE eid=2 AND SLOT=-1");
            assertThrows(ArithmeticException.class, () -> store.write(rows(update(1, 2, 1), update(2, 2, 2))));
            assertEquals(0L, query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT=-1 AND eid=1").get(0).get(0));
            assertEquals(0L, query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT>=0 AND eid=1").get(0).get(0));
            assertEquals(1L, query("SELECT CREATED_TIME_MS FROM HB_STATE WHERE SLOT=-1 AND eid=2").get(0).get(0));
            assertEquals(1L, query("SELECT MAX(DB_SEQUENCE) FROM HB_STATE WHERE SLOT>=0 AND eid=2").get(0).get(0));
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
                    "SELECT CREATED_TIME_MS, sendtime, SEND_TIME_RAW, DB_SEQUENCE FROM HB_STATE WHERE SLOT=-1 AND eid=66").get(0));
            assertEquals(2L, query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT>=0 AND eid=66").get(0).get(0));
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
        List<?> latest = query("SELECT CREATED_TIME_MS, DB_SEQUENCE FROM HB_STATE WHERE SLOT=-1 AND eid=88").get(0);
        assertEquals(2L, latest.get(0));
        assertEquals(latest.get(1), query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT>=0 AND eid=88").get(0).get(0));
        assertEquals(latest.get(1), query("SELECT MAX(DB_SEQUENCE) FROM HB_STATE WHERE SLOT>=0 AND eid=88").get(0).get(0));
    }

    @Test void overlappingBatchesLockElevatorsInTheSameOrderAndKeepNewestState() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int n = 1; n <= 2; n++) {
            final int time = n;
            futures.add(CompletableFuture.runAsync(() -> {
                try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
                    barrier.await(10, TimeUnit.SECONDS);
                    store.write(time == 1
                            ? rows(update(1, time, time), update(2, time, time), update(3, time, time))
                            : rows(update(3, time, time), update(2, time, time), update(1, time, time)));
                } catch (Exception error) { throw new RuntimeException(error); }
            }));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
        for (int id = 1; id <= 3; id++) {
            List<?> latest = query("SELECT CREATED_TIME_MS, DB_SEQUENCE FROM HB_STATE WHERE SLOT=-1 AND eid=" + id).get(0);
            assertEquals(2L, latest.get(0));
            assertEquals(latest.get(1), query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT>=0 AND eid=" + id).get(0).get(0));
        }
    }

    @Test void coldTableUsesCustomNamedDefaultRegionWithPageEviction() {
        // SYS.CACHES exposes the omitted cache option as NULL, not the resolved default name.
        assertEquals(Collections.singletonList(Collections.singletonList(null)),
                query("SELECT DATA_REGION_NAME FROM SYS.CACHES WHERE CACHE_NAME='HB_STATE'"));
        DataRegionConfiguration region = server.configuration().getDataStorageConfiguration()
                .getDefaultDataRegionConfiguration();
        assertFalse(region.isPersistenceEnabled());
        assertEquals("Memory_Region", region.getName());
        assertEquals("Memory_Region", ((org.apache.ignite.internal.IgniteEx) server).context().cache()
                .internalCache("HB_STATE").context().group().dataRegion().config().getName());
        assertEquals(org.apache.ignite.configuration.DataPageEvictionMode.RANDOM_2_LRU, region.getPageEvictionMode());
        assertEquals(0.7, region.getEvictionThreshold());
        assertTrue(server.configuration().getDataStorageConfiguration().getDataRegionConfigurations() == null
                || server.configuration().getDataStorageConfiguration().getDataRegionConfigurations().length == 0);
    }

    @Test void partialWindowLossDoesNotPreventNewHeartbeats() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            for (int n = 1; n <= 10; n++) store.write(rows(update(999, n, n)));
            // Deterministic record-loss simulation; this does not predict which pages LRU will evict.
            query("DELETE FROM HB_STATE WHERE EID=999 AND SLOT IN (-1,3,4)");
            assertEquals(1, store.write(rows(update(999, 11, 11))));
            assertEquals(11L, query("SELECT CREATED_TIME_MS FROM HB_STATE WHERE EID=999 AND SLOT=-1").get(0).get(0));
            assertEquals(1L, query("SELECT DB_SEQUENCE FROM HB_STATE WHERE EID=999 AND SLOT=-1").get(0).get(0));
            for (int n = 12; n <= 22; n++) store.write(rows(update(999, n, n)));
            assertEquals(11L, query("SELECT COUNT(*) FROM HB_STATE WHERE EID=999").get(0).get(0));
            assertEquals(13L, query("SELECT MIN(CREATED_TIME_MS) FROM HB_STATE WHERE EID=999 AND SLOT>=0").get(0).get(0));
        }
    }

    @Test void memoryRestartLosesWindowAndNewHeartbeatsRebuildItWithExistingClient() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            store.write(rows(update(99, 1, 1)));
            store.write(rows(update(100, 1, 1)));
            server.close();
            startServer();
            assertFalse(server.cacheNames().contains(IgniteHeartbeatStore.Schema.CACHE));
            assertEquals(1, store.write(rows(update(99, 2, 2))));
            assertEquals(1L, query("SELECT DB_SEQUENCE FROM HB_STATE WHERE SLOT=-1 AND eid=99").get(0).get(0));
            assertEquals(0L, query("SELECT COUNT(*) FROM HB_STATE WHERE eid=100").get(0).get(0));
            for (int n = 3; n <= 15; n++) store.write(rows(update(99, n, n)));
            assertEquals(11L, query("SELECT COUNT(*) FROM HB_STATE WHERE eid=99").get(0).get(0));
            assertEquals(6L, query("SELECT MIN(CREATED_TIME_MS) FROM HB_STATE WHERE SLOT>=0 AND eid=99").get(0).get(0));
            assertEquals(15L, query("SELECT CREATED_TIME_MS FROM HB_STATE WHERE SLOT=-1 AND eid=99").get(0).get(0));
        }
    }

    @Test void sqlTimestampsKeepMillisNullsAndUseCreatedDateRangeIndex() throws Exception {
        long created = java.time.Instant.parse("2026-11-04T16:00:00.123Z").toEpochMilli();
        long updated = java.time.Instant.parse("2026-11-04T15:59:59.456Z").toEpochMilli();
        ElevatorUpdate first = update(700, created, 1);
        first.state.createdDate = "2026/11/5 0:00:00.123";
        first.state.updatedDate = "2026/11/4 23:59:59.456";
        first.sendTimeMs = created + 789;
        IgniteHeartbeatStore.Row copied;
        java.util.TimeZone original = java.util.TimeZone.getDefault();
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/New_York"));
            copied = IgniteHeartbeatStore.Row.copyOf(first);
        } finally { java.util.TimeZone.setDefault(original); }
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            store.write(Arrays.asList(copied, IgniteHeartbeatStore.Row.copyOf(update(701, created + 1000, 1))));
        }
        List<?> stored = query("SELECT CREATEDDATE, UPDATEDDATE, CREATED_TIME_MS, PART_DAY FROM HB_STATE WHERE EID=700 AND SLOT=-1").get(0);
        assertEquals(java.sql.Timestamp.class, stored.get(0).getClass());
        assertEquals(created, ((java.sql.Timestamp) stored.get(0)).getTime());
        assertEquals(updated, ((java.sql.Timestamp) stored.get(1)).getTime());
        assertEquals(created, stored.get(2));
        assertEquals("2026-11-05", stored.get(3));
        assertNull(query("SELECT UPDATEDDATE FROM HB_STATE WHERE EID=701 AND SLOT=-1").get(0).get(0));
        for (List<?> sent : query("SELECT SENDTIME FROM HB_STATE WHERE EID=700 ORDER BY SLOT")) {
            assertEquals(java.sql.Timestamp.class, sent.get(0).getClass());
            assertEquals(created + 789, ((java.sql.Timestamp) sent.get(0)).getTime());
        }
        java.sql.Timestamp start = new java.sql.Timestamp(created), end = new java.sql.Timestamp(created + 1000);
        assertEquals(Collections.singletonList(Collections.singletonList(700L)),
                query("SELECT EID FROM HB_STATE WHERE SLOT=-1 AND CREATEDDATE>=? AND CREATEDDATE<? ORDER BY CREATEDDATE", start, end));
        String plan = query("EXPLAIN SELECT EID FROM HB_STATE WHERE CREATEDDATE>=? AND CREATEDDATE<?", start, end).toString();
        assertTrue(plan.contains(IgniteHeartbeatStore.Schema.CREATED_DATE_INDEX), plan);
    }

    @Test void startupRecreatesMissingIndexButReadOnlyCheckDoesNot() {
        query("DROP INDEX PUBLIC.IDX_HB_STATE_CREATEDDATE");
        try {
            assertThrows(IllegalStateException.class, () -> new IgniteHeartbeatStore(settings, false));
        } finally {
            try (IgniteHeartbeatStore ignored = new IgniteHeartbeatStore(settings)) { }
        }
        try (IgniteHeartbeatStore ignored = new IgniteHeartbeatStore(settings, false)) { }
    }

    @Test void incorrectNamedIndexIsRejectedWithoutBeingSilentlyReplaced() {
        query("DROP INDEX PUBLIC.IDX_HB_STATE_CREATEDDATE");
        query("CREATE INDEX IDX_HB_STATE_CREATEDDATE ON PUBLIC.HB_STATE(UPDATEDDATE)");
        try {
            assertThrows(IllegalStateException.class, () -> new IgniteHeartbeatStore(settings));
            assertTrue(query("SELECT COLUMNS FROM SYS.INDEXES WHERE INDEX_NAME='IDX_HB_STATE_CREATEDDATE'")
                    .toString().contains("UPDATEDDATE"));
        } finally {
            query("DROP INDEX PUBLIC.IDX_HB_STATE_CREATEDDATE");
            try (IgniteHeartbeatStore ignored = new IgniteHeartbeatStore(settings)) { }
        }
    }

    @Test void oldStringSchemaIsRejectedAndRecreationUsesANewBinaryValueType() throws Exception {
        String ddl = Files.readString(java.nio.file.Paths.get("sql/ignite2-heartbeat.sql"))
                .replaceAll("(?m)^--.*$", "").split(";")[0]
                .replace(IgniteHeartbeatStore.Schema.VALUE_TYPE, "HeartbeatStateRowV2")
                .replace("SENDTIME TIMESTAMP", "SENDTIME BIGINT")
                .replace("CREATEDDATE TIMESTAMP", "CREATEDDATE VARCHAR")
                .replace("UPDATEDDATE TIMESTAMP", "UPDATEDDATE VARCHAR");
        query("DROP TABLE PUBLIC.HB_STATE");
        try {
            query(ddl);
            query("INSERT INTO HB_STATE(EID,SLOT,CREATEDDATE,UPDATEDDATE) VALUES(800,-1,'old','old')");
            assertThrows(IllegalStateException.class, () -> new IgniteHeartbeatStore(settings));
            assertEquals("old", query("SELECT CREATEDDATE FROM HB_STATE WHERE EID=800").get(0).get(0));
        } finally {
            query("DROP TABLE PUBLIC.HB_STATE");
            try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
                store.write(rows(update(800, 1234, 1)));
            }
        }
        assertEquals(new java.sql.Timestamp(1234), query("SELECT CREATEDDATE FROM HB_STATE WHERE EID=800 AND SLOT=-1").get(0).get(0));
    }

    @Test void oldBigintSendTimeIsRejectedAndRecreationKeepsOldMetadataSeparate() throws Exception {
        String ddl = Files.readString(java.nio.file.Paths.get("sql/ignite2-heartbeat.sql"))
                .replaceAll("(?m)^--.*$", "").split(";")[0]
                .replace(IgniteHeartbeatStore.Schema.VALUE_TYPE, "HeartbeatStateRowV3")
                .replace("SENDTIME TIMESTAMP", "SENDTIME BIGINT");
        query("DROP TABLE PUBLIC.HB_STATE");
        try {
            query(ddl);
            query("INSERT INTO HB_STATE(EID,SLOT,SENDTIME) VALUES(801,-1,1234)");
            assertThrows(IllegalStateException.class, () -> new IgniteHeartbeatStore(settings));
            assertEquals(1234L, query("SELECT SENDTIME FROM HB_STATE WHERE EID=801").get(0).get(0));
        } finally {
            query("DROP TABLE PUBLIC.HB_STATE");
            try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
                store.write(rows(update(801, 1234, 1)));
            }
        }
        assertEquals(new java.sql.Timestamp(1234), query("SELECT SENDTIME FROM HB_STATE WHERE EID=801 AND SLOT=-1").get(0).get(0));
    }

    @Test void readOnlyCheckDoesNotCreateMissingTableAndConcurrentWritersCanCreateIt() throws Exception {
        server.destroyCache(IgniteHeartbeatStore.Schema.CACHE);
        assertThrows(IllegalStateException.class, () -> new IgniteHeartbeatStore(settings, false));
        assertFalse(server.cacheNames().contains(IgniteHeartbeatStore.Schema.CACHE));
        CyclicBarrier barrier = new CyclicBarrier(3);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int n = 1; n <= 3; n++) {
            final int id = n;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
                        store.write(rows(update(id, 1, 1)));
                    }
                } catch (Exception error) { throw new RuntimeException(error); }
            }));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
        assertEquals(3L, query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT=-1").get(0).get(0));
        try (IgniteHeartbeatStore ignored = new IgniteHeartbeatStore(settings, false)) { }
    }

    @Test void startupRejectsAtomicCacheAndMissingFields() {
        ClientCacheConfiguration config;
        try (org.apache.ignite.client.IgniteClient client = Ignition.startClient(
                new org.apache.ignite.configuration.ClientConfiguration().setAddresses(settings.addresses))) {
            config = client.cache(IgniteHeartbeatStore.Schema.CACHE).getConfiguration();
        }
        config.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        assertThrows(IllegalStateException.class, () -> IgniteHeartbeatStore.Schema.validate(config));
        config.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        config.setDataRegionName("Memory_Region");
        assertDoesNotThrow(() -> IgniteHeartbeatStore.Schema.validate(config));
        config.getQueryEntities()[0].getFields().remove("SPEED");
        assertThrows(IllegalStateException.class, () -> IgniteHeartbeatStore.Schema.validate(config));
    }

    static void removeTableForPackagedColdStart() {
        server.destroyCache(IgniteHeartbeatStore.Schema.CACHE);
    }

    @Test void realFlinkSinkFlushesShortFinalBatch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        IgniteHeartbeatSink.attach(env.fromCollection(Arrays.asList(update(11, 1, 1), update(11, 2, 2), update(33, 1, 1))), settings);
        env.execute("heartbeat-ignite-integration");
        assertEquals(2L, query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT=-1").get(0).get(0));
        assertEquals(3L, query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT>=0").get(0).get(0));
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
            assertEquals(1L, query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT>=0 AND eid=44").get(0).get(0));
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
            assertEquals(Arrays.asList(2L, 2L), query("SELECT CREATED_TIME_MS, DB_SEQUENCE FROM HB_STATE WHERE SLOT=-1 AND eid=44").get(0));
            assertEquals(2L, query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT>=0 AND eid=44").get(0).get(0));
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
