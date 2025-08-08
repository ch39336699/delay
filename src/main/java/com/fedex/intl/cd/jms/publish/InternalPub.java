package com.fedex.intl.cd.jms.publish;

import com.fedex.intl.cd.defines.ResourceDefineBase;
import com.fedex.intl.cd.scheduled.Statistics;
import com.fedex.intl.cd.utils.DelayedMessageStats;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Service;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.util.Map;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Service
public class InternalPub {

    private final String name = "internalQueue";

    @Autowired()
    private DelayedMessageStats delayedMessageStats;

    @Autowired
    public JmsTemplate jmsTemplate;

    @Autowired
    public Statistics statistics;

    @Value("${" + name + ".name}")
    String queueName;

    public void sendMessage(byte[] messagePayLoad, @Headers Map<String, Object> headers, String uuid, String trackItemNumber) {
        boolean successful = false;
        String rulesfile = "";
        String stateType = "";
        int retryCount = 0;
        try {
            jmsTemplate.send(queueName, new MessageCreator() {
                public Message createMessage(Session session) throws JMSException {
                    BytesMessage bmsg = session.createBytesMessage();
                    bmsg.clearBody();
                    bmsg.writeBytes(messagePayLoad);
                    if (headers != null) {
                        for (Map.Entry<String, Object> entry : headers.entrySet()) {
                            //When putting a message BACK onto the internal queue we need to remove scheduledJobId, else the message won't get delayed
                            //https://activemq.apache.org/delay-and-schedule-message-delivery
                            if (!entry.getKey().trim().equals("scheduledJobId")) {
                                try {
                                    bmsg.setObjectProperty(entry.getKey(), entry.getValue());
                                } catch (Exception ex) {
                                    String value = entry.getValue().toString();
                                    bmsg.setObjectProperty(entry.getKey(), value.trim());
                                }
                            }
                        }
                    }
                    bmsg.setObjectProperty("UUID", uuid.toString());
                    return bmsg;
                }
            });
            if (headers.containsKey(ResourceDefineBase.RULES_FILE) && headers.get(ResourceDefineBase.RULES_FILE) != null) {
                rulesfile = (String) headers.get(ResourceDefineBase.RULES_FILE);
            }
            if (headers.containsKey(ResourceDefineBase.STATE_TYPE) && headers.get(ResourceDefineBase.STATE_TYPE) != null) {
                stateType = (String) headers.get(ResourceDefineBase.STATE_TYPE);
            }
            if ((headers.containsKey(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT)) && (headers.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT).toString().trim().length() > 0)) {
                retryCount = Integer.parseInt(headers.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT).toString());
            }
            JSONObject main = new JSONObject();
            main.put("queue", queueName);
            main.put("uuid", uuid);
            main.put("trackItemNumber", trackItemNumber);
            main.put("rulesfile", rulesfile);
            main.put("stateType", stateType);
            main.put("retryCount", retryCount);
            if (log.isDebugEnabled()) {
                log.debug("InternalPub.sendMessage(), Published. ", kv("data", main));
            }
            if (headers.containsKey(ResourceDefineBase.EXTERNAL_PUB)) {
                delayedMessageStats.enqueueRetryMessage(headers, uuid.toString(), trackItemNumber);
            } else {
                delayedMessageStats.enqueueMessage(messagePayLoad, headers, uuid.toString(), trackItemNumber);
            }
            successful = true;
        } catch (Exception ex) {
            log.error("InternalPub.sendMessage(), Exception:", ex);
        } finally {
            if (successful) {
                statistics.xltNormalMsgCount++;
            }
        }
    }
}
