package com.hitachi.yx2602.heartbeat;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/** Launched in a separate JVM with only the shaded JAR and this test helper on its classpath. */
public final class PackagedIgniteClient {
    public static void main(String[] args) throws Exception {
        Properties p = new Properties();
        p.setProperty("kafka.bootstrap.servers", "unused:9092");
        p.setProperty("kafka.input.topic", "input");
        p.setProperty("kafka.error.topic", "errors");
        p.setProperty("kafka.group.id", "packaged-smoke");
        p.setProperty("checkpoint.storage", "file:///unused");
        p.setProperty("ignite.addresses", args[0]);
        IgniteConfig config = ReaderConfig.from(p, key -> null).ignite;
        ElevatorUpdate update = new ElevatorUpdate();
        update.elevatorId = 123; update.createdTimeMs = 1000; update.sendTimeMs = null; update.historySequence = 99;
        update.state = new ElevatorState(); update.state.mainFaultCode = "0008";
        try (IgniteHeartbeatStore store = new IgniteHeartbeatStore(config)) {
            if (store.write(Collections.singletonList(IgniteHeartbeatStore.Row.copyOf(update))) != 1) throw new AssertionError("first write");
            if (store.write(Collections.singletonList(IgniteHeartbeatStore.Row.copyOf(update))) != 0) throw new AssertionError("replay");
        }
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(args[0]));
             org.apache.ignite.cache.query.FieldsQueryCursor<List<?>> cursor = client.query(new SqlFieldsQuery(
                     "SELECT DB_SEQUENCE, FLINK_SEQUENCE, mainfaultcode, speed FROM PUBLIC.HB_STATE WHERE SLOT=-1 AND eid=123"))) {
            if (!java.util.Arrays.asList(1L, 99L, "0008", null).equals(cursor.getAll().get(0))) {
                throw new AssertionError("packaged SQL mapping");
            }
        }
        if (!update.toString().contains("0008")) throw new AssertionError("shaded Jackson");
        String source = IgniteHeartbeatStore.class.getProtectionDomain().getCodeSource().getLocation().toString();
        if (!source.endsWith("heartbeat-reader-0.1.0.jar")) throw new AssertionError("wrong application classpath: " + source);
        System.out.println("PACKAGED_IGNITE_SMOKE_OK");
    }
}
