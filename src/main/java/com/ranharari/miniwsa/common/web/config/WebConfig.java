package com.ranharari.miniwsa.common.web.config;

import com.ranharari.miniwsa.common.web.GlobalExceptionHandler;
import com.ranharari.miniwsa.common.web.RequestIdFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** HTTP pieces every role shares. Spring Boot adds any Filter bean to the servlet chain. */
@Configuration
public class WebConfig {

    @Bean
    RequestIdFilter requestIdFilter() {
        return new RequestIdFilter();
    }

    @Bean
    GlobalExceptionHandler globalExceptionHandler() {
        return new GlobalExceptionHandler();
    }
}
