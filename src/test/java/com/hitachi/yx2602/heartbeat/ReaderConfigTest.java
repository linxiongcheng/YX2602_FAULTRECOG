package com.hitachi.yx2602.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ReaderConfigTest {
    static Properties properties() {
        Properties p = new Properties();
        p.setProperty("kafka.bootstrap.servers", "localhost:9092");
        p.setProperty("kafka.input.topic", "heartbeat-test");
        p.setProperty("kafka.error.topic", "heartbeat-test-errors");
        p.setProperty("kafka.group.id", "heartbeat-reader-test");
        p.setProperty("checkpoint.storage", "file:///tmp/heartbeat-test-checkpoints");
        return p;
    }

    @Test void defaultsProtectProgressAndProducerReliability() {
        ReaderConfig config = ReaderConfig.from(properties(), key -> null);
        assertEquals(60000, config.futureSkewMs);
        assertEquals("false", config.consumerProperties().getProperty("enable.auto.commit"));
        assertEquals("all", config.producerProperties().getProperty("acks"));
        assertEquals("true", config.producerProperties().getProperty("enable.idempotence"));
        assertTrue(config.checkpointTimeoutMs > config.deliveryTimeoutMs);
    }

    @Test void rejectsInputErrorTopicLoop() {
        Properties p = properties();
        p.setProperty("kafka.error.topic", p.getProperty("kafka.input.topic"));
        assertThrows(IllegalArgumentException.class, () -> ReaderConfig.from(p, key -> null));
    }

    @Test void expandsEnvironmentWithoutCorruptingJaasEscapes() {
        Properties p = properties();
        p.setProperty("kafka.client.sasl.jaas.config", "${JAAS}");
        String secret = "module required password=\"$1\\foo\";";
        ReaderConfig config = ReaderConfig.from(p, key -> secret);
        assertEquals(secret, config.producerProperties().getProperty("sasl.jaas.config"));
        assertThrows(IllegalArgumentException.class, () -> ReaderConfig.from(p, key -> null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"enable.auto.commit", "acks", "retries", "group.id", "auto.offset.reset"})
    void cannotOverrideDeliveryContract(String key) {
        Properties p = properties();
        p.setProperty("kafka.client." + key, "bad");
        assertThrows(IllegalArgumentException.class, () -> ReaderConfig.from(p, name -> null));
    }

    @Test void rejectsTypoAndInvalidTimeouts() {
        Properties p = properties();
        p.setProperty("paralellism", "3");
        assertThrows(IllegalArgumentException.class, () -> ReaderConfig.from(p, key -> null));
        p.remove("paralellism");
        p.setProperty("checkpoint.timeout.ms", "100000");
        assertThrows(IllegalArgumentException.class, () -> ReaderConfig.from(p, key -> null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "1.5", "true", "9223372036854775808"})
    void rejectsInvalidSkew(String value) {
        Properties p = properties();
        p.setProperty("validation.max.future.skew.ms", value);
        assertThrows(IllegalArgumentException.class, () -> ReaderConfig.from(p, key -> null));
    }
}
