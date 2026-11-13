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

    @Test void discoversAllTopicsAndNeverConfusesSameNumberedPartitions() {
        java.util.Properties p = ReaderConfigTest.properties();
        p.setProperty("kafka.input.topic", "heartbeat1,heartbeat2,heartbeat3");
        ReaderConfig config = ReaderConfig.from(p, key -> null);
        Map<String, org.apache.kafka.clients.admin.TopicDescription> topics = new HashMap<>();
        for (String name : config.inputTopics) {
            java.util.List<org.apache.kafka.common.TopicPartitionInfo> partitions = Arrays.asList(
                    new org.apache.kafka.common.TopicPartitionInfo(0, null, Collections.emptyList(), Collections.emptyList()),
                    new org.apache.kafka.common.TopicPartitionInfo(1, null, Collections.emptyList(), Collections.emptyList()));
            topics.put(name, new org.apache.kafka.clients.admin.TopicDescription(name, false, partitions));
        }
        java.util.Set<TopicPartition> partitions = HeartbeatReader.inputPartitions(config, topics);
        assertEquals(6, partitions.size());
        TopicPartition old = new TopicPartition("heartbeat3", 0);
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(old, new OffsetAndMetadata(100));
        offsets.put(new TopicPartition("unrelated", 0), new OffsetAndMetadata(999));
        java.util.Set<TopicPartition> missing = HeartbeatReader.missingPartitions(partitions, offsets);
        assertEquals(5, missing.size());
        assertFalse(missing.contains(old));
        assertTrue(missing.contains(new TopicPartition("heartbeat1", 0)));
        assertTrue(missing.contains(new TopicPartition("heartbeat2", 0)));
        assertEquals(100, offsets.get(old).offset());
    }
}
