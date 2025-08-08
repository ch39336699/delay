package com.fedex.intl.cd.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
public class ScheduledTaskConfig implements SchedulingConfigurer {
    private final int POOL_SIZE = 10;

    @Override
    public void configureTasks(ScheduledTaskRegistrar scheduledTaskRegistrar) {
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(POOL_SIZE);
        threadPoolTaskScheduler.setThreadNamePrefix("polling-pool-");
        threadPoolTaskScheduler.initialize();
        threadPoolTaskScheduler.setAwaitTerminationSeconds(150);
        threadPoolTaskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduledTaskRegistrar.setTaskScheduler(threadPoolTaskScheduler);
    }
}
