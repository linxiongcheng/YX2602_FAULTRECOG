package com.hitachi.yx2602.heartbeat;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OffsetBootstrapTest {
    @Test void initializesOnlyMissingPartitionsIncludingNewPartitions() {
        TopicPartition existing = new TopicPartition("raw", 0);
        TopicPartition added = new TopicPartition("raw", 1);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(existing, new OffsetAndMetadata(0));
        offsets.put(added, null);
        assertEquals(Collections.singleton(added), HeartbeatReader.missingPartitions(
                new HashSet<>(Arrays.asList(existing, added)), offsets));
        assertEquals(0, offsets.get(existing).offset());
    }
}
