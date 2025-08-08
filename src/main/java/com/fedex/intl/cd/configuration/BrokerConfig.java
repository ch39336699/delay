package com.fedex.intl.cd.configuration;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.AbortSlowAckConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.StorePendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;

import javax.jms.ConnectionFactory;
import java.io.File;
import java.util.List;

@Configuration
public class BrokerConfig {


    @Autowired
    private Environment environment;

    @Value("${broker.directory}")
    String brokerDir;

    @Value("${broker.port:61616}")
    String brokerPort;

    @Value("${broker.useAsyncSend:true}")
    String useAsyncSend;

    @Value("${broker.prefetchPolicy:50}")
    String prefetchPolicy;

    @Value("${broker.socketBufferSize:231072}")
    String socketBufferSize;

    @Value("${broker.persistent:false}")
    boolean persistent;


    /* Active MQ Embedded Broker configuration */
    @Bean
    public BrokerService broker() throws Exception {
        File storeDir = null;
        File schedulerDir = null;
        String port = null;
        String[] profiles = this.environment.getActiveProfiles();
        if (profiles.length > 1) {
            port = profiles[1];
        }
        if ((port != null) && !port.equals("null")) {
            storeDir = new File(brokerDir + port + "/store");
            schedulerDir = new File(brokerDir + port + "/scheduler");
        } else {
            storeDir = new File(brokerDir + "/store");
            schedulerDir = new File(brokerDir + "/scheduler");
        }
        final BrokerService broker = new BrokerService();
        PersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(storeDir);
        broker.setDeleteAllMessagesOnStartup(false);
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.setDataDirectoryFile(schedulerDir);
        broker.getSystemUsage().getMemoryUsage().setLimit(64 * 2024 * 2024 * 1000L);
        broker.getSystemUsage().getStoreUsage().setLimit(2024 * 2024 * 2024 * 4000L);
        broker.getSystemUsage().getTempUsage().setLimit(2024 * 2024 * 2024 * 4000L);
        broker.setPersistent(persistent);
        broker.setSchedulerSupport(true);
        //broker.setUseJmx(true);
        AbortSlowAckConsumerStrategy strategy = new AbortSlowAckConsumerStrategy();
        strategy.setAbortConnection(true);
        //strategy.setCheckPeriod(checkPeriod);
       // strategy.setMaxSlowDuration(maxSlowDuration);
       // strategy.setMaxTimeSinceLastAck(maxTimeSinceLastAck);
        PolicyEntry policyEntry = new PolicyEntry();
       // policyEntry.setCursorMemoryHighWaterMark(50);
       // policyEntry.setExpireMessagesPeriod(600000);
        policyEntry.setOptimizedDispatch(true);
       // policyEntry.setProducerFlowControl(true);
        policyEntry.setMaxPageSize(150);
        //policyEntry.setPendingDurableSubscriberPolicy(new StorePendingDurableSubscriberMessageStoragePolicy());
        policyEntry.setMemoryLimit(64 * 2024 * 2024L * 1000L);
        policyEntry.setProducerFlowControl(false);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry( policyEntry );
        broker.setDestinationPolicy( policyMap );
        if ((port != null) && !port.equals("null")) {
            broker.addConnector("tcp://0.0.0.0:" + port + "?socketBufferSize=231072" + "?ioBufferSize=26384" + "?jms.prefetchPolicy.all=80" + "?jms.useAsyncSend=true" + "?jms.optimizeAcknowledge=true" + "?consumer.dispatchAsync=false" + "?wireFormat.cacheSize=4048" + "?async=false");
           // broker.addConnector("tcp://0.0.0.0:" + port + "?socketBufferSize=131072" + "jms.prefetchPolicy.all=" + prefetchPolicy + "?jms.useAsyncSend=" + useAsyncSend);
        } else {
            broker.addConnector("tcp://0.0.0.0:" + brokerPort + "?socketBufferSize=231072"  + "?ioBufferSize=26384" + "?jms.prefetchPolicy.all=80" + "?jms.useAsyncSend=true" + "?jms.optimizeAcknowledge=true" + "?consumer.dispatchAsync=false" + "?wireFormat.cacheSize=4048" + "?async=false");
        }
        broker.setUseShutdownHook(true);
        return broker;
    }

    /* Consumer configuration */
    @Bean
    public JmsListenerContainerFactory<?> myFactory(
            ConnectionFactory connectionFactory,
            DefaultJmsListenerContainerFactoryConfigurer configurer) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        // This provides all boot's default to this factory, including the message converter
        configurer.configure(factory, connectionFactory);
        // You could still override some of Boot's default if necessary.
        return factory;
    }

    @Bean
    public MessageConverter jacksonJmsMessageConverter() {
        MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
        converter.setTargetType(MessageType.TEXT);
        converter.setTypeIdPropertyName("_type");
        return converter;
    }
}
