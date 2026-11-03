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
    private IgniteClient client;
    private ClientCache<Long, BinaryObject> latest;
    private ClientCache<BinaryObject, BinaryObject> history;

    IgniteHeartbeatStore(IgniteConfig config) {
        this.config = config;
        config.requireAddresses();
        historySettings = new IgniteConfig.HistorySettings(config);
        try { connect(); }
        catch (RuntimeException error) { historySettings.close(); throw error; }
    }

    private void connect() {
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
            latest = client.<Long, BinaryObject>cache(Schema.LATEST).withKeepBinary();
            history = client.<BinaryObject, BinaryObject>cache(Schema.HISTORY).withKeepBinary();
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
                // 连接异常可能发生在提交已成功但响应丢失之后；整批重试由库内时间比较防止重复写入。
                disconnect();
                if (attempt >= config.attempts) throw failure;
                Thread.sleep(config.retryDelayMs);
            }
        }
    }

    private int writeTransaction(List<Row> rows) {
        int accepted = 0;
        // 一批只读取一次配置快照，配置刷新不会改变已开始事务的策略。
        IgniteConfig.HistoryPolicy requested = historySettings.current();
        try (ClientTransaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ, config.transactionTimeoutMs)) {
            for (Row row : rows) {
                // 悲观事务的可重复读也锁定不存在的键，避免并发首次写入都取得序号 1。
                BinaryObject previous = latest.get(row.elevatorId);
                // 数据库再次比较 CreatedDate，拦截检查点恢复重放以及事务重试产生的重复或旧数据。
                if (previous != null && row.createdTimeMs <= (Long) previous.field("CREATED_TIME_MS")) continue;
                // 数据库序号独立递增，不依赖 Flink 恢复后的序号。
                long sequence = previous == null ? 1 : Math.addExact((Long) previous.field("DB_SEQUENCE"), 1);
                IgniteConfig.HistoryPolicy effective = effectivePolicy(previous, requested);
                if (previous != null && effective.maxRecords != (Integer) previous.field("HISTORY_LIMIT")) {
                    resizeHistory(row.elevatorId, (Integer) previous.field("HISTORY_LIMIT"), effective.maxRecords);
                }
                BinaryObject value = value(row, sequence, effective);
                BinaryObject key = historyKey(row.elevatorId, (int) ((sequence - 1) % effective.maxRecords));
                // 最新状态、历史槽位及序号在同一事务中提交，避免两张表只成功更新一张。
                latest.put(row.elevatorId, value);
                history.put(key, value);
                accepted++;
            }
            tx.commit();
        }
        return accepted;
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
        IgniteConfig.historyLimit(oldLimit);
        Set<BinaryObject> oldKeys = new HashSet<>();
        for (int slot = 0; slot < oldLimit; slot++) oldKeys.add(historyKey(elevatorId, slot));
        List<BinaryObject> retained = new ArrayList<>(history.getAll(oldKeys).values());
        retained.sort(Comparator.comparingLong(value -> (Long) value.field("DB_SEQUENCE")));
        // 为即将写入的新记录预留一格。先取出最近的记录，再按新上限重排槽位，避免直接改取模造成误覆盖。
        int from = Math.max(0, retained.size() - (newLimit - 1));
        Map<BinaryObject, BinaryObject> replacement = new LinkedHashMap<>();
        for (int index = from; index < retained.size(); index++) {
            BinaryObject value = retained.get(index);
            long sequence = value.field("DB_SEQUENCE");
            replacement.put(historyKey(elevatorId, (int) ((sequence - 1) % newLimit)), value);
        }
        history.removeAll(oldKeys);
        if (!replacement.isEmpty()) history.putAll(replacement);
    }

    private BinaryObject historyKey(long elevatorId, int slot) {
        return client.binary().builder(Schema.HISTORY_KEY_TYPE)
                .setField("ELEVATOR_ID", elevatorId, Long.class)
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
        static final String LATEST = "HB_LATEST", HISTORY = "HB_HISTORY", REGION = "heartbeat";
        static final String VALUE_TYPE = "HeartbeatStateV1", HISTORY_KEY_TYPE = "HeartbeatHistoryKeyV1";
        static final Map<String, Field> STATE_FIELDS;
        static final Map<String, Class<?>> VALUE_FIELDS;

        static {
            Map<String, Field> stateFields = new LinkedHashMap<>();
            // 显式列出持久化字段，新增解析字段时须同步核对映射和建表脚本。
            String names = "intId fId code floor acd direction rs doorLock isActive isDebug isRepair isPower "
                    + "isDoor isHardware isCall runTotal runTime speed mainFaultCode subFaultCode doorCounter "
                    + "secDoorCouter weight tcda tcdb tcdc tcdStopOutsideDoor tcdOpenDoorRuning tcdEntrapment "
                    + "tcdDoor sendCount receiveCount signalRssi networkType recordStatus status createdDate updatedDate";
            try {
                for (String name : names.split(" ")) {
                    String column = "STATE_" + name.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toUpperCase(Locale.ROOT);
                    stateFields.put(column, ElevatorState.class.getField(name));
                }
            } catch (NoSuchFieldException e) { throw new ExceptionInInitializerError(e); }
            STATE_FIELDS = Collections.unmodifiableMap(stateFields);
            Map<String, Class<?>> valueFields = new LinkedHashMap<>();
            valueFields.put("PORT", Integer.class);
            valueFields.put("SEND_TIME_MS", Long.class);
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
            for (String name : Arrays.asList(LATEST, HISTORY)) {
                if (!caches.contains(name)) throw new IllegalStateException("缺少缓存" + name + "，请先执行sql/ignite2-heartbeat.sql");
                validate(client.cache(name).getConfiguration(), name);
            }
        }

        static void validate(ClientCacheConfiguration config, String name) {
            boolean history = HISTORY.equals(name);
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
            require((history ? HISTORY_KEY_TYPE : Long.class.getName()).equals(entity.getKeyType()), name, "KEY_TYPE");
            Map<String, String> expected = new LinkedHashMap<>();
            expected.put("ELEVATOR_ID", Long.class.getName());
            if (history) expected.put("SLOT", Integer.class.getName());
            VALUE_FIELDS.forEach((column, type) -> expected.put(column, type.getName()));
            require(expected.equals(entity.getFields()), name, "字段名称及类型，须与CreatedDate建表脚本完全一致");
            if (entity.getNotNullFields() != null) {
                require(Collections.disjoint(entity.getNotNullFields(), STATE_FIELDS.keySet()), name, "业务字段必须允许NULL");
            }
            if (history) {
                require(new HashSet<>(Arrays.asList("ELEVATOR_ID", "SLOT")).equals(entity.getKeyFields()), name, "历史表复合主键");
            } else require("ELEVATOR_ID".equals(entity.getKeyFieldName()), name, "最新表主键");
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
        final Map<String, Object> values;

        private Row(ElevatorUpdate update) {
            if (update.elevatorId <= 0 || update.createdTimeMs <= 0 || update.state == null) {
                throw new IllegalArgumentException("写入必须包含有效电梯ID、CreatedDate时间和状态");
            }
            elevatorId = update.elevatorId;
            createdTimeMs = update.createdTimeMs;
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("PORT", update.port);
            fields.put("SEND_TIME_MS", update.sendTimeMs);
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
