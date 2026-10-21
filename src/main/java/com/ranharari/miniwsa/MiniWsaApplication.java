package com.ranharari.miniwsa;

import com.ranharari.miniwsa.alerting.config.AlertingConfig;
import com.ranharari.miniwsa.common.config.AppConfig;
import com.ranharari.miniwsa.common.config.KafkaSerializationConfig;
import com.ranharari.miniwsa.common.config.KafkaStreamsConfig;
import com.ranharari.miniwsa.common.config.KafkaTopicsConfig;
import com.ranharari.miniwsa.common.web.config.WebConfig;
import com.ranharari.miniwsa.enrich.config.EnrichConfig;
import com.ranharari.miniwsa.ingest.config.IngestConfig;
import com.ranharari.miniwsa.processor.config.ProcessorConfig;
import com.ranharari.miniwsa.query.config.QueryConfig;
import com.ranharari.miniwsa.sink.config.SinkConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Import;

/**
 * No classpath scanning: the app is exactly these modules. Each config creates its module's beans
 * explicitly, and its {@code @Profile} decides whether the module runs in this container.
 */
@SpringBootConfiguration
@EnableAutoConfiguration
@Import({
        AppConfig.class, WebConfig.class,
        KafkaTopicsConfig.class, KafkaSerializationConfig.class, KafkaStreamsConfig.class,
        IngestConfig.class,
        EnrichConfig.class, ProcessorConfig.class,
        SinkConfig.class,
        QueryConfig.class,
        AlertingConfig.class})
public class MiniWsaApplication {

    public static void main(String[] args) {
        SpringApplication.run(MiniWsaApplication.class, args);
    }
}
