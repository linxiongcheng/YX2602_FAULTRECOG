package com.hitachi.yx2602.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class IgniteConfigTest {
    @Test void previewRemainsUsableWithoutDatabaseButProductionRequiresAddresses() {
        IgniteConfig config = ReaderConfig.from(ReaderConfigTest.properties(), key -> null).ignite;
        assertThrows(IllegalArgumentException.class, config::requireAddresses);
    }

    @Test void acceptsAddressesAndExpandsCredentials() {
        Properties p = ReaderConfigTest.properties();
        p.setProperty("ignite.addresses", " node1:10800 , [::1]:10800 ");
        p.setProperty("ignite.username", "writer");
        p.setProperty("ignite.password", "${IGNITE_PASS}");
        IgniteConfig config = ReaderConfig.from(p, key -> "$1\\secret").ignite;
        config.requireAddresses();
        assertEquals("$1\\secret", config.password);
    }

    @ParameterizedTest @ValueSource(strings = {"node1", "node1:0", "node1:65536", "node1:10800,", "http://node1:10800"})
    void rejectsBadAddresses(String address) {
        Properties p = ReaderConfigTest.properties();
        p.setProperty("ignite.addresses", address);
        assertThrows(IllegalArgumentException.class, () -> ReaderConfig.from(p, key -> null));
    }

    @Test void rejectsUnboundedOrInconsistentBufferAndTimeoutSettings() {
        for (String[] entry : new String[][] {
                {"ignite.queue.capacity", "1"}, {"ignite.batch.size", "0"},
                {"ignite.flush.timeout.ms", "180000"}, {"ignite.transaction.timeout.ms", "60000"},
                {"ignite.retry.attempts", "0"}, {"ignite.ssl.enabled", "yes"},
                {"ignite.username", "user-without-password"}, {"ignite.adresses", "typo:10800"}}) {
            Properties p = ReaderConfigTest.properties(); p.setProperty(entry[0], entry[1]);
            assertThrows(IllegalArgumentException.class, () -> ReaderConfig.from(p, key -> null), entry[0]);
        }
    }

    @Test void schemaCoversEveryParserBusinessField() {
        assertEquals(java.util.Arrays.stream(ElevatorState.class.getFields())
                .filter(field -> !java.lang.reflect.Modifier.isStatic(field.getModifiers())).count(),
                IgniteSchema.STATE_FIELDS.size());
    }
}
