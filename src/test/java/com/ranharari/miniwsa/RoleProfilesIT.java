package com.ranharari.miniwsa;

import com.ranharari.miniwsa.ingest.IngestController;
import com.ranharari.miniwsa.processor.EnrichmentTopology;
import com.ranharari.miniwsa.query.StatsController;
import com.ranharari.miniwsa.sink.EnrichedEventListener;
import com.ranharari.miniwsa.support.PipelineContainers;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/** Each container role (Spring profile) loads its own module and nothing else; no profile loads all. */
class RoleProfilesIT {

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        PipelineContainers.register(registry);
    }

    @Nested
    @SpringBootTest
    @ActiveProfiles("ingest")
    class Ingest {
        @Autowired
        ApplicationContext context;

        @Test
        void loadsOnlyIngest() {
            assertThat(context.getBeanNamesForType(IngestController.class)).hasSize(1);
            assertThat(context.getBeanNamesForType(EnrichmentTopology.class)).isEmpty();
            assertThat(context.getBeanNamesForType(EnrichedEventListener.class)).isEmpty();
            assertThat(context.getBeanNamesForType(StatsController.class)).isEmpty();
        }
    }

    @Nested
    @SpringBootTest
    @ActiveProfiles("processor")
    class Processor {
        @Autowired
        ApplicationContext context;

        @Test
        void loadsOnlyProcessor() {
            assertThat(context.getBeanNamesForType(EnrichmentTopology.class)).hasSize(1);
            assertThat(context.getBeanNamesForType(IngestController.class)).isEmpty();
            assertThat(context.getBeanNamesForType(EnrichedEventListener.class)).isEmpty();
            assertThat(context.getBeanNamesForType(StatsController.class)).isEmpty();
        }
    }

    @Nested
    @SpringBootTest
    @ActiveProfiles("sink")
    class Sink {
        @Autowired
        ApplicationContext context;

        @Test
        void loadsOnlySink() {
            assertThat(context.getBeanNamesForType(EnrichedEventListener.class)).hasSize(1);
            assertThat(context.getBeanNamesForType(IngestController.class)).isEmpty();
            assertThat(context.getBeanNamesForType(EnrichmentTopology.class)).isEmpty();
            assertThat(context.getBeanNamesForType(StatsController.class)).isEmpty();
        }
    }

    @Nested
    @SpringBootTest
    @ActiveProfiles("query")
    class Query {
        @Autowired
        ApplicationContext context;

        @Test
        void loadsOnlyQuery() {
            assertThat(context.getBeanNamesForType(StatsController.class)).hasSize(1);
            assertThat(context.getBeanNamesForType(IngestController.class)).isEmpty();
            assertThat(context.getBeanNamesForType(EnrichmentTopology.class)).isEmpty();
            assertThat(context.getBeanNamesForType(EnrichedEventListener.class)).isEmpty();
        }
    }

    @Nested
    @SpringBootTest
    class NoProfile {
        @Autowired
        ApplicationContext context;

        @Test
        void loadsEveryModule() {
            assertThat(context.getBeanNamesForType(IngestController.class)).hasSize(1);
            assertThat(context.getBeanNamesForType(EnrichmentTopology.class)).hasSize(1);
            assertThat(context.getBeanNamesForType(EnrichedEventListener.class)).hasSize(1);
            assertThat(context.getBeanNamesForType(StatsController.class)).hasSize(1);
        }
    }
}
