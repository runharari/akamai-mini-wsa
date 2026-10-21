package com.ranharari.miniwsa;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

/**
 * Spring finds only the {@code @Configuration} classes (one per module, in its {@code config} package).
 * They create every bean explicitly; the active profile decides which modules load. All other classes
 * are plain Java, so reading a module's config shows its whole wiring.
 */
@SpringBootConfiguration
@EnableAutoConfiguration
@ComponentScan(useDefaultFilters = false,
        includeFilters = @Filter(Configuration.class),
        // The same excludes as @SpringBootApplication, so test slices (@WebMvcTest) still work.
        excludeFilters = {
                @Filter(type = FilterType.CUSTOM, classes = TypeExcludeFilter.class),
                @Filter(type = FilterType.CUSTOM, classes = AutoConfigurationExcludeFilter.class)})
public class MiniWsaApplication {

    public static void main(String[] args) {
        SpringApplication.run(MiniWsaApplication.class, args);
    }
}
