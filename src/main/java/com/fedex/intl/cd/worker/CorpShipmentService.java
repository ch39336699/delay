package com.fedex.intl.cd.worker;

import com.fedex.intl.cd.jms.publish.CorpShipmentTopic;
import com.fedex.intl.cd.jms.publish.MDERetryQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import javax.jms.Message;
import java.util.Map;

/**
 Service class for Processing XLT Messages from Local ActiveMQ */
@Slf4j
//@Profile({"none"})
@Component("com.fedex.intl.cd.worker.CorpShipmentService")
public class CorpShipmentService extends MessageProcessorBase {
    @Autowired
    private CorpShipmentTopic corpShipmentTopic;

    public boolean inUse;
    public int runCount;
    public long lastRunTime;


    /***********************************************
     *     Abstract Methods
     *************************************************/
    public String getProcessorName() {
        return null;
    }


    public void publisherSendMessage(byte[] messagePayLoad, @Headers Map <String, Object> headers, long messageID, String uuid, String trackItemNumber) throws Exception {
        try {
            corpShipmentTopic.send(messagePayLoad, headers, uuid, trackItemNumber);
        } catch (Exception e) {
            log.warn("CorpShipmentTopic.send(byte[]), Producer is null for {} ...initializing and trying again.", corpShipmentTopic.queueName);
            corpShipmentTopic.closeConnections();
            corpShipmentTopic.setup();
            corpShipmentTopic.send(messagePayLoad, headers, uuid, trackItemNumber);
        }
    }

    public void publisherSendMessage(Message message, @Headers Map <String, Object> headers, long messageID, String uuid, String trackItemNumber) throws Exception {
    }
}
