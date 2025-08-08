package com.fedex.intl.cd.jms.receive;

import com.fedex.intl.cd.defines.ResourceDefineBase;
import com.fedex.intl.cd.jms.publish.InternalPub;
import com.fedex.intl.cd.model.DelayedMessage;
import com.fedex.intl.cd.worker.CorpShipmentService;
import com.fedex.intl.cd.worker.MDERetryService;
import com.fedex.intl.cd.worker.XLTNormalService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Service;

import javax.jms.JMSException;
import javax.jms.Message;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@Order(2)
@Profile({"local", "dev", "release", "volume", "production"})
public class RecvJMSConsumer extends ReceiverBase {

    @Autowired
    InternalPub internalPub;

    private final String name = "recvQueue";

    @Value("${" + name + ".concurrency}")
    private String concurrency;

    //Interval between recovery attempts, in milliseconds. 30 seconds is what EIT requires.
    @Value("${" + name + ".recoveryInterval:30000}")
    public String recoveryInterval;

    @Value("${" + name + ".factory}")
    public String factoryName;

    @Value("${" + name + ".name}")
    public String queueName;

    @Value("${" + name + ".userName}")
    protected String ldapUserName;

    @Value("${" + name + ".encryptedPW}")
    protected String encryptedQueuePassword;

    @Autowired
    MDERetryService mdeRetryService;
    @Autowired
    CorpShipmentService corpShipmentService;

    public int receivedCnt;

    @Bean(name = "recvMessageFactory")
    public JmsListenerContainerFactory<DefaultMessageListenerContainer> recvMessageFactory() {
        try {
            return super.createMessageFactory(name, queueName, factoryName, concurrency, Long.parseLong(recoveryInterval), ldapUserName, encryptedQueuePassword);
        } catch (Exception ex) {
            log.error("RecvJMSConsumer.recvMessageFactory(), Exception: ", ex);
        }
        return null;
    }


    @JmsListener(destination = "${recvQueue.name}", containerFactory = "recvMessageFactory", concurrency = "${recvQueue.concurrency}")
    public void receiveMessage(Message inputMessage, @Headers Map<String, Object> headers) {
        byte[] messagePayLoad;
        UUID uuid = UUID.randomUUID();
        XLTNormalService xltNormalService = null;
        DelayedMessage message = null;
        String trackItemNumber = "n/a";
        try {
            readConfig();
            messagePayLoad = getMessagePayload(inputMessage, queueName);
            if (headers.containsKey(ResourceDefineBase.TRACK_ITEM_NBR) && (headers.get(ResourceDefineBase.TRACK_ITEM_NBR) != null)) {
                trackItemNumber = (String) headers.get(ResourceDefineBase.TRACK_ITEM_NBR);
            }
            if (log.isDebugEnabled()) {
                log.debug("RecvJMSConsumer.receiveMessage(), Received Message. UUID: {}, Headers: {}", uuid, headers.toString());
            }
        } catch (Exception ex) {
            log.error("RecvJMSConsumer().receiveMessage(), Throwing away message. Exception:", ex);
            return;
        }
        try {
            if (messagePayLoad != null) {
                receivedCnt++;
                if (PRINT_MSGS_RECEIVED) {
                    printMsg(messagePayLoad, headers, uuid.toString(), trackItemNumber);
                }
                if (headers.containsKey(ResourceDefineBase.EXTERNAL_PUB)) {
                    String key = ((String) headers.get(ResourceDefineBase.EXTERNAL_PUB)).trim();
                    if ((key != null) && (key.length() > 0)) {
                        switch (key) {
                            case "MDE_RETRY":
                                if (mdeRetryService.onMessageExternal(inputMessage, messagePayLoad, headers, message)) {
                                } else {
                                    log.warn("InternalRecv.receiveMessage(), Error sending MDE_RETRY message.");
                                }
                                break;
                            case "CORP_SHIP":
                                if (corpShipmentService.onMessageExternal(inputMessage, messagePayLoad, headers, message)) {
                                } else {
                                    log.warn("InternalRecv.receiveMessage(), Error sending CORP_SHIP message.");
                                }
                                break;
                            default:
                                // code block
                        }
                    } else {
                        log.warn("InternalRecv.receiveMessage(), No value set in properties for externalPub");
                    }
                } else {
                    synchronized (this._available) {
                        xltNormalService = this.acquireReusable();
                    }
                    if (xltNormalService.onMessageExternal(inputMessage, messagePayLoad, headers, message)) {
                    } else {
                        log.warn("InternalRecv.receiveMessage(), Error sending XLTNormal message.");
                    }
                    if (xltNormalService != null) {
                        synchronized (this._available) {
                            this.releaseReusable(xltNormalService);
                        }
                    }
                }
            } else {
                log.error("RecvJMSConsumer.receiveMessage(), messagePayLoad is null.");
            }
            try {
                //If not exception thrown(no rollback) then we need to ack the message for AMQ.
                inputMessage.acknowledge();
            } catch (JMSException ex) {
                //If Tibco we'll probably get an exception. Don't need to Log anything. Just keep going.
            }
        } catch (Exception rtex) {
            //Catch and throw back to JMS to put messages back on the queue.
            log.error("RecvJMSConsumer.receiveMessage(), Rolling message back to JMS queue. RuntimeException: {}", rtex.getMessage());
        }
    }

}
