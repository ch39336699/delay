package com.fedex.intl.cd.configuration.database;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties("oracle.cdv")
public class OracleProperties {
    private String password;
    private String username;
    private String url;
    private Integer maximumPoolSize;
    private Integer minPoolSize;
    private Integer initialPoolSize;
    private Integer maxStatements;
    private Integer connectionWaitTimeout;
    private Integer timeoutCheckInterval;
    private Integer timeToLiveConnectionTimeout;
    private Integer inactiveConnectionTimeout;
    private Integer abandonedConnectionTimeout;
    private Integer maxConnectionReuseCount;
}
