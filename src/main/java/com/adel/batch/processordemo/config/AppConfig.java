package com.adel.batch.processordemo.config;

import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//@Configuration
public class AppConfig {

    /*
     * Use the standard Cassandra driver API to create a com.datastax.oss.driver.api.core.CqlSession instance.
     */
    @Bean
    public CqlSession session() {
        return CqlSession.builder().withKeyspace("example").build();
    }
}
