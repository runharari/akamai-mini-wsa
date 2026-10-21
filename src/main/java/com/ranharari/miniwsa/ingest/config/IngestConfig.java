package com.ranharari.miniwsa.ingest.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** Settings of the ingest role. */
@Configuration
@Profile({"ingest", "default"})
@EnableConfigurationProperties(IngestProperties.class)
public class IngestConfig {
}
