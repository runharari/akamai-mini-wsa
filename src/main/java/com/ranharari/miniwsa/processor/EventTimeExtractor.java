package com.ranharari.miniwsa.processor;

import com.ranharari.miniwsa.common.model.SecurityEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Makes Kafka Streams use the event's own timestamp (when the attack happened) instead of the
 * time the message was written to Kafka. Stream time, and so the idle-IP eviction, follows event time.
 */
class EventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof SecurityEvent event && event.timestamp() != null) {
            return event.timestamp().toEpochMilli();
        }
        return record.timestamp();
    }
}
