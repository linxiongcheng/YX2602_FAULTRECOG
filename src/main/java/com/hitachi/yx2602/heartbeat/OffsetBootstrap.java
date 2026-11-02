package com.hitachi.yx2602.heartbeat;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Persist the first latest position before processing, closing the pre-first-checkpoint gap. */
final class OffsetBootstrap {
    private OffsetBootstrap() { }

    static void initialize(ReaderConfig config) throws Exception {
        Properties properties = config.connectionProperties();
        properties.setProperty("bootstrap.servers", config.bootstrapServers);
        properties.setProperty("request.timeout.ms", "30000");
        properties.setProperty("default.api.timeout.ms", "60000");
        try (AdminClient admin = AdminClient.create(properties)) {
            // Both topics must already exist; never accidentally create an error loop or typo topic.
            Map<String, TopicDescription> topics = admin.describeTopics(
                    java.util.Arrays.asList(config.inputTopic, config.errorTopic))
                    .allTopicNames().get(60, TimeUnit.SECONDS);
            Set<TopicPartition> partitions = topics.get(config.inputTopic).partitions().stream()
                    .map(partition -> new TopicPartition(config.inputTopic, partition.partition()))
                    .collect(Collectors.toSet());
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(config.groupId)
                    .partitionsToOffsetAndMetadata().get(60, TimeUnit.SECONDS);
            Set<TopicPartition> missing = missingPartitions(partitions, committed);
            if (missing.isEmpty()) return;
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            for (TopicPartition partition : missing) request.put(partition, OffsetSpec.latest());
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = admin.listOffsets(request)
                    .all().get(60, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetAndMetadata> initial = new HashMap<>();
            latest.forEach((partition, offset) -> initial.put(partition,
                    new OffsetAndMetadata(offset.offset(), "yx2602-initial-latest")));
            // Only absent offsets are initialized. This is a start boundary, not a processed ACK.
            // Operational requirement: exactly one submitting/active job per consumer group.
            admin.alterConsumerGroupOffsets(config.groupId, initial).all().get(60, TimeUnit.SECONDS);
        }
    }

    static Set<TopicPartition> missingPartitions(Set<TopicPartition> partitions,
                                                Map<TopicPartition, OffsetAndMetadata> committed) {
        return partitions.stream().filter(partition -> committed.get(partition) == null)
                .collect(Collectors.toSet());
    }
}
