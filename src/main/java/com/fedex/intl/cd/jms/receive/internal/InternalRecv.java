package com.fedex.intl.cd.jms.receive.internal;

import com.fedex.intl.cd.defines.ResourceDefineBase;
import com.fedex.intl.cd.jms.receive.ReceiverBase;
import com.fedex.intl.cd.utils.AMQErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.Session;

@Slf4j
@Service
@Order(Ordered.HIGHEST_PRECEDENCE)
public class InternalRecv extends ReceiverBase {

    private final String name = "internalQueue";

    @Value("${" + name + ".name}")
    String queueName;

    //Interval between recovery attempts, in milliseconds. 30 seconds is what EIT requires.
    @Value("${" + name + ".recoveryInterval:30000}")
    public String recoveryInterval;

    @Autowired()
    private AMQErrorHandler errorHandler;
    @Value("${" + name + ".concurrency:5}")
    private String concurrency;

    @Bean(name = "amqMessageFactory")
    public JmsListenerContainerFactory amqMessageFactory(ConnectionFactory connectionFactory, DefaultJmsListenerContainerFactoryConfigurer configurer) {

        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setErrorHandler(t -> {
            log.info("InternalRecv.amqMessageFactory(), An error has occurred in the transaction");
            log.error("InternalRecv.amqMessageFactory(), {}", t.getCause().getMessage());
        });

        factory.setConcurrency(concurrency);
        factory.setRecoveryInterval(Long.parseLong(recoveryInterval));
        factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
        //Guaranteed redelivery in case of a user exception thrown as well as in case of other listener execution
        //Interruptions (such as the JVM dying)
        factory.setSessionTransacted(false);
        factory.setErrorHandler(errorHandler);
        configurer.configure(factory, connectionFactory);
        return factory;
    }

    @JmsListener(destination = "${internalQueue.name}", containerFactory = "amqMessageFactory")
    public void receiveMessage(@Payload Message msg) {
        try {
            msg.acknowledge();
            synchronized (this._available) {
                int retryCnt = 0;
                if (msg.propertyExists(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT)) {
                    retryCnt = Integer.parseInt(msg.getStringProperty(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT));
                }
                if (hasDuplicate((String) msg.getStringProperty("UUID") + "-" + retryCnt)) {
                    return;
                }
            }
        } catch (Exception ex2) {
        }
        try {
            String uuid = msg.getStringProperty("UUID");
            log.info("--------------------------------------- RECV INTERNAL {} ---------------------------------------", uuid);
            internalProcess(msg);
        } catch (Exception ex2) {
        }
    }


}
