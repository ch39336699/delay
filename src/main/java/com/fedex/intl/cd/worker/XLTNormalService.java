package com.fedex.intl.cd.worker;

import com.fedex.intl.cd.jms.publish.XLTNormalQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;
import javax.jms.*;

import java.util.Map;

/**
 Service class for Processing XLT Messages from Local ActiveMQ */
@Slf4j
@Component("com.fedex.intl.cd.worker.XLTNormalService")
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class XLTNormalService extends MessageProcessorBase {
    @Autowired
    private XLTNormalQueue xltNormalExternalPublisher;

    public boolean inUse;
    public int runCount;
    public long lastRunTime;


    /***********************************************
     *     Abstract Methods
     *************************************************/
    public String getProcessorName() {
        return null;
    }

    /***********************************************
     *     For External queues via EIT
     *************************************************/
    public void publisherSendMessage(byte[] messagePayLoad, @Headers Map <String, Object> headers, long messageID, String uuid, String trackItemNumber) throws Exception {
        try {
            xltNormalExternalPublisher.send(messagePayLoad, headers, messageID, uuid, trackItemNumber);
        } catch (Exception e) {
            log.warn("XLTNormalService.send(byte[]), Producer is null for {} ...initializing and trying again.", xltNormalExternalPublisher.queueName);
            xltNormalExternalPublisher.closeConnections();
            xltNormalExternalPublisher.setup();
            xltNormalExternalPublisher.send(messagePayLoad, headers, messageID, uuid, trackItemNumber);
        }
    }

    public void publisherSendMessage(Message message, @Headers Map <String, Object> headers, long messageID, String uuid, String trackItemNumber) throws Exception {
        try {
            xltNormalExternalPublisher.send(message, headers, messageID, uuid, trackItemNumber);
        } catch (Exception e) {
            log.warn("XLTNormalService.send(byte[]), Producer is null for {} ...initializing and trying again.", xltNormalExternalPublisher.queueName);
            xltNormalExternalPublisher.closeConnections();
            xltNormalExternalPublisher.setup();
            xltNormalExternalPublisher.send(message, headers, messageID, uuid, trackItemNumber);
        }
    }
}
