package com.fedex.intl.cd.utils.pools;

import com.fedex.intl.cd.worker.XLTNormalService;
import org.springframework.aop.target.CommonsPool2TargetSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration
@EnableAspectJAutoProxy(proxyTargetClass = true)
@ComponentScan(basePackages = "com.fedex")
public class PoolConfiguration {

    private final String name = "target";

    @Value("${" + name + ".pool.maxSize}")
    private int maxSize;

    @Value("${" + name + ".pool.minIdle}")
    private int minIdle;

    @Value("${" + name + ".pool.minEvictableIdleTime}")
    private int minEvictableIdleTime;

    @Value("${" + name + ".pool.timeBetweenEvictionRuns}")
    private int timeBetweenEvictionRuns;

    @Bean("TargetPool")
    public CommonsPool2TargetSource xltNormalTargetSource() {
        final CommonsPool2TargetSource commonsPoolTargetSource = new CommonsPool2TargetSource();
        commonsPoolTargetSource.setTargetBeanName("com.fedex.intl.cd.worker.XLTNormalService");
        commonsPoolTargetSource.setTargetClass(XLTNormalService.class);
        commonsPoolTargetSource.setMaxSize(maxSize);
        commonsPoolTargetSource.setMinIdle(minIdle);
        commonsPoolTargetSource.setMinEvictableIdleTimeMillis(minEvictableIdleTime);
        commonsPoolTargetSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRuns);
        return commonsPoolTargetSource;
    }

}

