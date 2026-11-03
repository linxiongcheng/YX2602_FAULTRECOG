package com.hitachi.yx2602.heartbeat;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** SQL is for provisioning and reading; all transactional mutations use the cache API. */
final class IgniteSchema {
    static final String LATEST = "HB_LATEST", HISTORY = "HB_HISTORY", REGION = "heartbeat";
    static final String VALUE_TYPE = "HeartbeatStateV1", HISTORY_KEY_TYPE = "HeartbeatHistoryKeyV1";
    static final int HISTORY_SIZE = 10;
    static final Map<String, Field> STATE_FIELDS;
    static final Map<String, Class<?>> VALUE_FIELDS;

    static {
        Map<String, Field> stateFields = new LinkedHashMap<>();
        // Explicit list makes a newly added parser field require an intentional schema migration.
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
        valueFields.put("FLINK_SEQUENCE", Long.class);
        valueFields.put("RECEIVED_AT_MS", Long.class);
        valueFields.put("PARSED_AT_MS", Long.class);
        valueFields.put("SOURCE_TOPIC", String.class);
        valueFields.put("SOURCE_PARTITION", Integer.class);
        valueFields.put("SOURCE_OFFSET", Long.class);
        valueFields.put("KAFKA_TIMESTAMP_MS", Long.class);
        valueFields.put("DB_SEQUENCE", Long.class);
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
        require(expected.equals(entity.getFields()), name, "字段名称及类型，须与版本1建表脚本完全一致");
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
