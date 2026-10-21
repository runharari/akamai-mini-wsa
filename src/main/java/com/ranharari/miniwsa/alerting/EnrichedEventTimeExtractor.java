package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.model.EnrichedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/** Counts events in the minute they happened (the event's own timestamp), not when they reached Kafka. */
class EnrichedEventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof EnrichedEvent enriched && enriched.event().timestamp() != null) {
            return enriched.event().timestamp().toEpochMilli();
        }
        return record.timestamp();
    }
}
