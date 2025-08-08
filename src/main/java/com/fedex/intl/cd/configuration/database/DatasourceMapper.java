package com.fedex.intl.cd.configuration.database;

import lombok.extern.slf4j.Slf4j;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration
@Slf4j
public class DatasourceMapper {
    /*
     * Oracle DB Settings
     * https://docs.oracle.com/cd/E18283_01/java.112/e12265/optimize.htm
     */

    @Bean(name = "oracleDataSource")
    public DataSource dataSource(OracleProperties oracleProperties) throws SQLException {
        PoolDataSource dataSource = PoolDataSourceFactory.getPoolDataSource();
        dataSource.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        dataSource.setUser(oracleProperties.getUsername());
        dataSource.setPassword(oracleProperties.getPassword());
        dataSource.setURL(oracleProperties.getUrl());
        dataSource.setFastConnectionFailoverEnabled(true);
        dataSource.setInitialPoolSize(oracleProperties.getInitialPoolSize());
        dataSource.setMinPoolSize(oracleProperties.getMinPoolSize());
        dataSource.setMaxPoolSize(oracleProperties.getMaximumPoolSize());
        dataSource.setMaxStatements(oracleProperties.getMaxStatements());
        dataSource.setTimeToLiveConnectionTimeout(oracleProperties.getTimeToLiveConnectionTimeout());
        dataSource.setConnectionWaitTimeout(oracleProperties.getConnectionWaitTimeout());
        dataSource.setMaxConnectionReuseCount(oracleProperties.getMaxConnectionReuseCount());
        dataSource.setInactiveConnectionTimeout(oracleProperties.getInactiveConnectionTimeout());
        return dataSource;
    }
}
