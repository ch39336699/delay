package com.fedex.intl.cd.worker;

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
@Component("com.fedex.intl.cd.worker.MDERetryService")
public class MDERetryService extends MessageProcessorBase {
    @Autowired
    private MDERetryQueue mdeRetryQueue;

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
            mdeRetryQueue.send(messagePayLoad, headers, messageID, uuid, trackItemNumber);
        } catch (Exception e) {
            log.warn("MDERetryService.send(byte[]), Producer is null for {} ...initializing and trying again.", mdeRetryQueue.queueName);
            mdeRetryQueue.closeConnections();
            mdeRetryQueue.setup();
            mdeRetryQueue.send(messagePayLoad, headers, messageID, uuid, trackItemNumber);
        }
    }

    public void publisherSendMessage(Message message, @Headers Map <String, Object> headers, long messageID, String uuid, String trackItemNumber) throws Exception {
    }
}
