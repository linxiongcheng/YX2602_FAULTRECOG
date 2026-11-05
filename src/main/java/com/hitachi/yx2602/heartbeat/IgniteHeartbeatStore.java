package com.hitachi.yx2602.heartbeat;

import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Ignite 存储实现：每个写入器独占客户端，事务在同一后台线程内开始、写入和提交。 */
final class IgniteHeartbeatStore implements AsyncHeartbeatWriter.Store {
    private final IgniteConfig config;
    private final IgniteConfig.HistorySettings historySettings;
    private final HeartbeatDiagnostics diagnostics;
    private IgniteClient client;
    private ClientCache<BinaryObject, BinaryObject> states;

    IgniteHeartbeatStore(IgniteConfig config) {
        this(config, HeartbeatDiagnostics.disabled());
    }

    IgniteHeartbeatStore(IgniteConfig config, HeartbeatDiagnostics diagnostics) {
        this.config = config;
        this.diagnostics = diagnostics;
        config.requireAddresses();
        historySettings = new IgniteConfig.HistorySettings(config);
        try { connect(); }
        catch (RuntimeException error) { historySettings.close(); throw error; }
    }

    private void connect() {
        long started = diagnostics.begin("connect");
        try { connectClient(); }
        finally { diagnostics.end("connect", started); }
    }

    private void connectClient() {
        ClientConfiguration connection = new ClientConfiguration()
                .setAddresses(java.util.Arrays.stream(config.addresses.split(",")).map(String::trim).toArray(String[]::new))
                .setTimeout(config.requestTimeoutMs)
                .setPartitionAwarenessEnabled(false)
                .setRetryPolicy(context -> false)
                .setSslMode(config.sslEnabled ? SslMode.REQUIRED : SslMode.DISABLED);
        if (!config.username.isEmpty()) connection.setUserName(config.username).setUserPassword(config.password);
        client = Ignition.startClient(connection);
        try {
            if (client.cluster().state() != org.apache.ignite.cluster.ClusterState.ACTIVE) {
                throw new IllegalStateException("Ignite集群必须处于ACTIVE可写状态");
            }
            Schema.validate(client);
            states = client.<BinaryObject, BinaryObject>cache(Schema.CACHE).withKeepBinary();
        } catch (RuntimeException e) { disconnect(); throw e; }
    }

    @Override public int write(List<Row> input) throws Exception {
        // 稳定排序保留同梯数据的到达顺序，并让并发事务按相同电梯顺序加锁，降低死锁风险。
        // 作业恢复时旧连接可能尚未断开，因此即便每个电梯已 keyBy，也需要统一加锁顺序。
        List<Row> rows = new ArrayList<>(input);
        rows.sort(Comparator.comparingLong(row -> row.elevatorId));
        for (int attempt = 1; ; attempt++) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Ignite writer cancelled");
            try {
                if (client == null) connect();
                return writeTransaction(rows);
            } catch (ClientException failure) {
                diagnostics.add("client_failures", 1);
                // 连接异常可能发生在提交已成功但响应丢失之后；整批重试由库内时间比较防止重复写入。
                disconnect();
                if (attempt >= config.attempts) throw failure;
                diagnostics.add("retries", 1);
                long started = diagnostics.begin("retry_backoff");
                try { Thread.sleep(config.retryDelayMs); }
                finally { diagnostics.end("retry_backoff", started); }
            }
        }
    }

    private int writeTransaction(List<Row> rows) {
        int accepted = 0;
        // 一批只读取一次配置快照，配置刷新不会改变已开始事务的策略。
        IgniteConfig.HistoryPolicy requested = historySettings.current();
        diagnostics.add("transaction_attempts", 1);
        try (ClientTransaction tx = startTransaction()) {
            Map<BinaryObject, BinaryObject> pending = new LinkedHashMap<>();
            BinaryObject latestKey = null;
            BinaryObject previous = null;
            long elevatorId = 0;
            for (Row row : rows) {
                if (latestKey == null || elevatorId != row.elevatorId) {
                    elevatorId = row.elevatorId;
                    latestKey = stateKey(elevatorId, Schema.LATEST_SLOT);
                    // 仍按电梯ID顺序获取固定键的事务锁，包括不存在的键。
                    // 同梯后续记录复用批内状态，避免重复网络读取；锁一直持有到提交。
                    long started = diagnostics.begin("latest_read");
                    try { previous = states.get(latestKey); }
                    finally { diagnostics.end("latest_read", started); }
                }
                // 数据库再次比较 CreatedDate，拦截检查点恢复重放以及事务重试产生的重复或旧数据。
                if (previous != null && row.createdTimeMs <= (Long) previous.field("CREATED_TIME_MS")) continue;
                // 数据库序号独立递增，不依赖 Flink 恢复后的序号。
                long sequence = previous == null ? 1 : Math.addExact((Long) previous.field("DB_SEQUENCE"), 1);
                IgniteConfig.HistoryPolicy effective = effectivePolicy(previous, requested);
                if (previous != null && effective.maxRecords != (Integer) previous.field("HISTORY_LIMIT")) {
                    resizeHistory(row.elevatorId, (Integer) previous.field("HISTORY_LIMIT"), effective.maxRecords);
                }
                BinaryObject value = value(row, sequence, effective);
                BinaryObject key = stateKey(row.elevatorId, (int) ((sequence - 1) % effective.maxRecords));
                // 按原始到达顺序逐条判断和递增序号；仅合并同一事务内对同一槽位的覆盖。
                // 最终历史与逐条put一致，但只将槽位最终值通过一次putAll发往服务端。
                pending.put(latestKey, value);
                pending.put(key, value);
                previous = value;
                accepted++;
            }
            if (!pending.isEmpty()) {
                long started = diagnostics.begin("bulk_write");
                try { states.putAll(pending); }
                finally { diagnostics.end("bulk_write", started); }
            }
            long started = diagnostics.begin("commit");
            try { tx.commit(); }
            finally { diagnostics.end("commit", started); }
        } catch (RuntimeException error) {
            diagnostics.add("transaction_failures", 1);
            throw error;
        }
        return accepted;
    }

    private ClientTransaction startTransaction() {
        long started = diagnostics.begin("transaction_start");
        try { return client.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ, config.transactionTimeoutMs); }
        finally { diagnostics.end("transaction_start", started); }
    }

    private IgniteConfig.HistoryPolicy effectivePolicy(BinaryObject previous, IgniteConfig.HistoryPolicy requested) {
        if (previous == null) return requested;
        Long version = previous.field("HISTORY_POLICY_VERSION");
        Integer limit = previous.field("HISTORY_LIMIT");
        if (version == null || limit == null) {
            throw new IllegalStateException("已存状态缺少历史策略字段，请使用当前建表结构和有效数据");
        }
        // 任务刷新或重启存在先后；库内更高版本优先，同版本不同值也不能覆盖已接受的策略。
        if (version > requested.version || (version == requested.version && version > 0)) {
            return new IgniteConfig.HistoryPolicy(version, limit);
        }
        return requested;
    }

    private void resizeHistory(long elevatorId, int oldLimit, int newLimit) {
        long started = diagnostics.begin("history_resize");
        try { resizeHistoryRows(elevatorId, oldLimit, newLimit); }
        finally { diagnostics.end("history_resize", started); }
    }

    private void resizeHistoryRows(long elevatorId, int oldLimit, int newLimit) {
        IgniteConfig.historyLimit(oldLimit);
        Set<BinaryObject> oldKeys = new HashSet<>();
        // 只调整非负历史槽位，始终保留用于串行化同梯事务的-1固定槽位。
        for (int slot = 0; slot < oldLimit; slot++) oldKeys.add(stateKey(elevatorId, slot));
        List<BinaryObject> retained = new ArrayList<>(states.getAll(oldKeys).values());
        retained.sort(Comparator.comparingLong(value -> (Long) value.field("DB_SEQUENCE")));
        // 为即将写入的新记录预留一格。先取出最近的记录，再按新上限重排槽位，避免直接改取模造成误覆盖。
        int from = Math.max(0, retained.size() - (newLimit - 1));
        Map<BinaryObject, BinaryObject> replacement = new LinkedHashMap<>();
        for (int index = from; index < retained.size(); index++) {
            BinaryObject value = retained.get(index);
            long sequence = value.field("DB_SEQUENCE");
            replacement.put(stateKey(elevatorId, (int) ((sequence - 1) % newLimit)), value);
        }
        states.removeAll(oldKeys);
        if (!replacement.isEmpty()) states.putAll(replacement);
    }

    private BinaryObject stateKey(long elevatorId, int slot) {
        return client.binary().builder(Schema.KEY_TYPE)
                .setField("EID", elevatorId, Long.class)
                .setField("SLOT", slot, Integer.class).build();
    }

    IgniteConfig.HistoryPolicy historyPolicy() { return historySettings.current(); }
    boolean historyConfigRefreshFailed() { return historySettings.lastRefreshFailed(); }

    private BinaryObject value(Row row, long sequence, IgniteConfig.HistoryPolicy policy) {
        BinaryObjectBuilder builder = client.binary().builder(Schema.VALUE_TYPE);
        for (Map.Entry<String, Class<?>> field : Schema.VALUE_FIELDS.entrySet()) {
            Object value;
            switch (field.getKey()) {
                case "DB_SEQUENCE": value = sequence; break;
                case "STORED_AT_MS": value = System.currentTimeMillis(); break;
                case "HISTORY_LIMIT": value = policy.maxRecords; break;
                case "HISTORY_POLICY_VERSION": value = policy.version; break;
                default: value = row.values.get(field.getKey());
            }
            setField(builder, field.getKey(), value, field.getValue());
        }
        return builder.build();
    }

    private static <T> void setField(BinaryObjectBuilder builder, String name, Object value, Class<T> type) {
        builder.setField(name, type.cast(value), type);
    }

    @Override public void close() {
        historySettings.close();
        disconnect();
    }

    private void disconnect() {
        if (client != null) { client.close(); client = null; }
    }

    /** 存储结构定义与启动校验：SQL 用于建表和查询，事务写入统一使用 Cache API。 */
    static final class Schema {
        static final String CACHE = "HB_STATE", REGION = "heartbeat";
        static final int LATEST_SLOT = -1;
        static final String VALUE_TYPE = "HeartbeatStateRowV2", KEY_TYPE = "HeartbeatStateKeyV2";
        static final Map<String, Field> STATE_FIELDS;
        static final Map<String, Class<?>> VALUE_FIELDS;

        static {
            Map<String, Field> stateFields = new LinkedHashMap<>();
            // 左侧保留文档拼写，再按无引号SQL标识符规则转大写；右侧为解析模型的Java字段名。
            String names = "IntId:intId FId:fId Code:code Floor:floor ACD:acd Direction:direction RS:rs "
                    + "DoorLock:doorLock IsActive:isActive IsDebug:isDebug IsRepair:isRepair IsPower:isPower "
                    + "IsDoor:isDoor IsHardware:isHardware IsCall:isCall RunTotal:runTotal RunTime:runTime "
                    + "Speed:speed MainFaultCode:mainFaultCode SubFaultCode:subFaultCode DoorCounter:doorCounter "
                    + "SecDoorCouter:secDoorCouter Weight:weight TCDA:tcda TCDB:tcdb TCDC:tcdc "
                    + "TCDStopOutsideDoor:tcdStopOutsideDoor TCDOpenDoorRuning:tcdOpenDoorRuning "
                    + "TCDEntrapment:tcdEntrapment TCDDoor:tcdDoor SendCount:sendCount ReceiveCount:receiveCount "
                    + "SignalRssi:signalRssi NetworkType:networkType RecordStatus:recordStatus Status:status "
                    + "CreatedDate:createdDate UpdatedDate:updatedDate";
            try {
                for (String name : names.split(" ")) {
                    String[] mapping = name.split(":");
                    stateFields.put(mapping[0].toUpperCase(Locale.ROOT), ElevatorState.class.getField(mapping[1]));
                }
            } catch (NoSuchFieldException e) { throw new ExceptionInInitializerError(e); }
            STATE_FIELDS = Collections.unmodifiableMap(stateFields);
            Map<String, Class<?>> valueFields = new LinkedHashMap<>();
            valueFields.put("PORT", Integer.class);
            valueFields.put("SENDTIME", Long.class);
            valueFields.put("PART_DAY", String.class);
            valueFields.put("SEND_TIME_RAW", String.class);
            valueFields.put("CREATED_TIME_MS", Long.class);
            valueFields.put("FLINK_SEQUENCE", Long.class);
            valueFields.put("RECEIVED_AT_MS", Long.class);
            valueFields.put("PARSED_AT_MS", Long.class);
            valueFields.put("SOURCE_TOPIC", String.class);
            valueFields.put("SOURCE_PARTITION", Integer.class);
            valueFields.put("SOURCE_OFFSET", Long.class);
            valueFields.put("KAFKA_TIMESTAMP_MS", Long.class);
            valueFields.put("DB_SEQUENCE", Long.class);
            valueFields.put("HISTORY_LIMIT", Integer.class);
            valueFields.put("HISTORY_POLICY_VERSION", Long.class);
            valueFields.put("STORED_AT_MS", Long.class);
            stateFields.forEach((column, field) -> valueFields.put(column, field.getType()));
            VALUE_FIELDS = Collections.unmodifiableMap(valueFields);
        }

        static void validate(IgniteClient client) {
            Set<String> caches = new HashSet<>(client.cacheNames());
            if (!caches.contains(CACHE)) throw new IllegalStateException("缺少缓存" + CACHE + "，请先执行sql/ignite2-heartbeat.sql");
            validate(client.cache(CACHE).getConfiguration());
        }

        static void validate(ClientCacheConfiguration config) {
            String name = CACHE;
            require(CACHE.equals(config.getName()), name, "缓存名");
            require(config.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL, name, "ATOMICITY=TRANSACTIONAL");
            require(config.getCacheMode() == CacheMode.PARTITIONED, name, "PARTITIONED");
            require(config.getWriteSynchronizationMode() == CacheWriteSynchronizationMode.FULL_SYNC, name, "FULL_SYNC");
            require(REGION.equals(config.getDataRegionName()), name, "DATA_REGION=heartbeat");
            require("PUBLIC".equals(config.getSqlSchema()) || "\"PUBLIC\"".equals(config.getSqlSchema()), name,
                    "SQL_SCHEMA=PUBLIC，实际=" + config.getSqlSchema());
            require(config.getExpiryPolicy() == null, name, "禁止配置过期策略");
            QueryEntity[] entities = config.getQueryEntities();
            require(entities != null && entities.length == 1, name, "唯一QueryEntity");
            QueryEntity entity = entities[0];
            require(name.equals(entity.getTableName()), name, "表名");
            require(VALUE_TYPE.equals(entity.getValueType()), name, "VALUE_TYPE=" + VALUE_TYPE);
            require(KEY_TYPE.equals(entity.getKeyType()), name, "KEY_TYPE=" + KEY_TYPE);
            Map<String, String> expected = new LinkedHashMap<>();
            expected.put("EID", Long.class.getName());
            expected.put("SLOT", Integer.class.getName());
            VALUE_FIELDS.forEach((column, type) -> expected.put(column, type.getName()));
            require(expected.equals(entity.getFields()), name, "字段名称、大小写及类型，须与HB_STATE建表脚本完全一致");
            if (entity.getNotNullFields() != null) {
                require(Collections.disjoint(entity.getNotNullFields(), STATE_FIELDS.keySet()), name, "业务字段必须允许NULL");
            }
            require(new HashSet<>(Arrays.asList("EID", "SLOT")).equals(entity.getKeyFields()), name, "复合主键(EID,SLOT)");
            if (entity.getAliases() != null) entity.getAliases().forEach((field, alias) ->
                    require(Objects.equals(field, alias), name, "禁止修改字段别名"));
        }

        private static void require(boolean valid, String cache, String requirement) {
            if (!valid) throw new IllegalStateException("Ignite结构不匹配: " + cache + "，要求" + requirement);
        }
    }

    /** 异步入队时生成不可变字段快照，隔离 Flink 对象复用对后台写入的影响。 */
    static final class Row {
        final long elevatorId;
        final long createdTimeMs;
        final long bufferedAtNanos = System.nanoTime();
        final Map<String, Object> values;

        private Row(ElevatorUpdate update) {
            if (update.elevatorId <= 0 || update.createdTimeMs <= 0 || update.state == null) {
                throw new IllegalArgumentException("写入必须包含有效电梯ID、CreatedDate时间和状态");
            }
            elevatorId = update.elevatorId;
            createdTimeMs = update.createdTimeMs;
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("PORT", update.port);
            fields.put("SENDTIME", update.sendTimeMs);
            fields.put("PART_DAY", Instant.ofEpochMilli(createdTimeMs)
                    .atZone(ZoneId.of("Asia/Shanghai")).toLocalDate().toString());
            fields.put("SEND_TIME_RAW", update.sendTimeRaw);
            fields.put("CREATED_TIME_MS", update.createdTimeMs);
            fields.put("FLINK_SEQUENCE", update.historySequence);
            fields.put("RECEIVED_AT_MS", update.receivedAtMs);
            fields.put("PARSED_AT_MS", update.parsedAtMs);
            fields.put("SOURCE_TOPIC", update.sourceTopic);
            fields.put("SOURCE_PARTITION", update.sourcePartition);
            fields.put("SOURCE_OFFSET", update.sourceOffset);
            fields.put("KAFKA_TIMESTAMP_MS", update.kafkaTimestampMs);
            for (Map.Entry<String, java.lang.reflect.Field> entry : Schema.STATE_FIELDS.entrySet()) {
                try { fields.put(entry.getKey(), entry.getValue().get(update.state)); }
                catch (IllegalAccessException e) { throw new IllegalStateException(e); }
            }
            values = java.util.Collections.unmodifiableMap(fields);
        }

        static Row copyOf(ElevatorUpdate update) { return new Row(update); }
    }
}
