package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;

/** Spring Kafka's entry point for the sink: hands every polled batch to {@link EventSink}. */
public class EnrichedEventListener {

    private final EventSink sink;

    public EnrichedEventListener(EventSink sink) {
        this.sink = sink;
    }

    @KafkaListener(id = "clickhouse-sink", topics = Topics.ENRICHED, batch = "true",
            concurrency = "${wsa.sink.concurrency:3}")
    public void onBatch(List<ConsumerRecord<String, EnrichedEvent>> records) {
        // An unreadable message arrives with a null value (ErrorHandlingDeserializer); EventSink skips it.
        sink.store(records.stream().map(ConsumerRecord::value).toList());
    }
}
