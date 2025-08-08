package com.fedex.intl.cd.configuration;

import com.fedex.intl.cd.endpoints.MoveEndpoint;
import com.fedex.intl.cd.endpoints.PurgeEndpoint;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class Endpoints {

    // Custom JMS Move endpoint for Spring Boot Admin
    @Bean
    public MoveEndpoint moveEndpoint() {
        return new MoveEndpoint();
    }

    // Custom JMS Purge endpoint for Spring Boot Admin
    @Bean
    public PurgeEndpoint purgeEndpoint() {
        return new PurgeEndpoint();
    }
}
