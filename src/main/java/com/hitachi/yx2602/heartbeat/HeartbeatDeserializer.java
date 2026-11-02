package com.hitachi.yx2602.heartbeat;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class HeartbeatDeserializer implements KafkaRecordDeserializationSchema<RawHeartbeat> {
    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawHeartbeat> out) {
        out.collect(new RawHeartbeat(record.value(), record.key(), record.topic(),
                record.partition(), record.offset(), record.timestamp()));
    }

    @Override
    public TypeInformation<RawHeartbeat> getProducedType() {
        return TypeInformation.of(RawHeartbeat.class);
    }
}
