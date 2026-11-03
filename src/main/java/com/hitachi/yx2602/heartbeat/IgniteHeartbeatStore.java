package com.hitachi.yx2602.heartbeat;

import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** A client belongs to one sink worker; Ignite thin-client transactions are thread-bound. */
final class IgniteHeartbeatStore implements HeartbeatStore {
    private final IgniteConfig config;
    private IgniteClient client;
    private ClientCache<Long, BinaryObject> latest;
    private ClientCache<BinaryObject, BinaryObject> history;

    IgniteHeartbeatStore(IgniteConfig config) {
        this.config = config;
        config.requireAddresses();
        connect();
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
            IgniteSchema.validate(client);
            latest = client.<Long, BinaryObject>cache(IgniteSchema.LATEST).withKeepBinary();
            history = client.<BinaryObject, BinaryObject>cache(IgniteSchema.HISTORY).withKeepBinary();
        } catch (RuntimeException e) { close(); throw e; }
    }

    @Override public int write(List<HeartbeatRow> input) throws Exception {
        // Stable sort preserves arrival order within an elevator and gives concurrent transactions
        // the same lock order, including when a previous job attempt has not disconnected yet.
        List<HeartbeatRow> rows = new ArrayList<>(input);
        rows.sort(Comparator.comparingLong(row -> row.elevatorId));
        for (int attempt = 1; ; attempt++) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Ignite writer cancelled");
            try {
                if (client == null) connect();
                return writeTransaction(rows);
            } catch (ClientException failure) {
                close();
                if (attempt >= config.attempts) throw failure;
                Thread.sleep(config.retryDelayMs);
            }
        }
    }

    private int writeTransaction(List<HeartbeatRow> rows) {
        int accepted = 0;
        try (ClientTransaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ, config.transactionTimeoutMs)) {
            for (HeartbeatRow row : rows) {
                // REPEATABLE_READ also locks an absent key: two first writes cannot both get sequence 1.
                BinaryObject previous = latest.get(row.elevatorId);
                if (previous != null && row.sendTimeMs <= (Long) previous.field("SEND_TIME_MS")) continue;
                long sequence = previous == null ? 1 : Math.addExact((Long) previous.field("DB_SEQUENCE"), 1);
                BinaryObject value = value(row, sequence);
                BinaryObject key = client.binary().builder(IgniteSchema.HISTORY_KEY_TYPE)
                        .setField("ELEVATOR_ID", row.elevatorId, Long.class)
                        .setField("SLOT", (int) ((sequence - 1) % IgniteSchema.HISTORY_SIZE), Integer.class).build();
                latest.put(row.elevatorId, value);
                history.put(key, value);
                accepted++;
            }
            tx.commit();
        }
        return accepted;
    }

    private BinaryObject value(HeartbeatRow row, long sequence) {
        BinaryObjectBuilder builder = client.binary().builder(IgniteSchema.VALUE_TYPE);
        for (Map.Entry<String, Class<?>> field : IgniteSchema.VALUE_FIELDS.entrySet()) {
            Object value = field.getKey().equals("DB_SEQUENCE") ? sequence
                    : field.getKey().equals("STORED_AT_MS") ? System.currentTimeMillis() : row.values.get(field.getKey());
            setField(builder, field.getKey(), value, field.getValue());
        }
        return builder.build();
    }

    private static <T> void setField(BinaryObjectBuilder builder, String name, Object value, Class<T> type) {
        builder.setField(name, type.cast(value), type);
    }

    @Override public void close() {
        if (client != null) { client.close(); client = null; }
    }
}
