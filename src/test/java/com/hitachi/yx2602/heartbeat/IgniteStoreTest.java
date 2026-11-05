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
import org.apache.ignite.configuration.WALMode;
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
        server.cache(IgniteHeartbeatStore.Schema.CACHE).clear();
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

    /** Count calls at the actual thin-client boundary while still using the real persistent server. */
    private static final class CacheProbe {
        final java.util.Map<String, Integer> calls = new java.util.LinkedHashMap<>();
        final List<Long> latestReads = new ArrayList<>();
        boolean failAfterBulkWrite;

        CacheProbe(IgniteHeartbeatStore store) throws Exception {
            java.lang.reflect.Field cache = IgniteHeartbeatStore.class.getDeclaredField("states");
            cache.setAccessible(true);
            Object delegate = cache.get(store);
            Object proxy = java.lang.reflect.Proxy.newProxyInstance(
                    org.apache.ignite.client.ClientCache.class.getClassLoader(),
                    new Class<?>[]{org.apache.ignite.client.ClientCache.class}, (ignored, method, args) -> {
                        calls.merge(method.getName(), 1, Integer::sum);
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

    @Test void durableRestartKeepsLatestAndHistorySlotsAndExistingClientCanReconnect() throws Exception {
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(settings)) {
            store.write(rows(update(99, 1, 1)));
            server.close();
            startServer();
            assertEquals(1L, query("SELECT CREATED_TIME_MS FROM HB_STATE WHERE SLOT=-1 AND eid=99").get(0).get(0));
            assertEquals(1L, query("SELECT COUNT(*) FROM HB_STATE WHERE SLOT>=0 AND eid=99").get(0).get(0));
            assertEquals(0, store.write(rows(update(99, 1, 1))));
            assertEquals(1, store.write(rows(update(99, 2, 2))));
            assertEquals(2L, query("SELECT DB_SEQUENCE FROM HB_STATE WHERE SLOT=-1 AND eid=99").get(0).get(0));
        }
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
        config.getQueryEntities()[0].getFields().remove("SPEED");
        assertThrows(IllegalStateException.class, () -> IgniteHeartbeatStore.Schema.validate(config));
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
