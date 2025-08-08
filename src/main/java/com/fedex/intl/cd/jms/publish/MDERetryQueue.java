package com.fedex.intl.cd.jms.publish;

import com.fedex.intl.cd.defines.ResourceDefineBase;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import java.util.Map;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Service
public class MDERetryQueue extends PublisherBase {
    private final String name = "mdeRetryQueue";

    @Value("${" + name + ".factory:null}")
    public String factoryName;

    @Value("${" + name + ".name:null}")
    public String topicName;

    @Value("${" + name + ".userName:null}")
    protected String ldapUserName;

    @Value("${" + name + ".encryptedPW:null}")
    protected String encryptedQueuePassword;

    @PostConstruct
    public void setup() {
        try {
            log.trace("MDERetryQueue.setup(), Called.");
            this.initialize(name, factoryName, "fxClientDestinationUID=D." + topicName, ldapUserName, encryptedQueuePassword);
        } catch (Exception ex) {
            log.error("MDERetryQueue.setup(), Exception:", ex);
        }
    }

    public void send(byte[] messagePayLoad, Map<String, Object> messageProperties, long messageID, String uuid, String trackItemNumber) throws JMSException {
        boolean sent = false;
        String rulesfile = "";
        String stateType = "";
        try {
            if ((producer == null) || (session == null)) {
                log.warn("MDERetryQueue.send(byte[]), {} Producer or Session is null  for messageID: {} ...initializing", messageID, name);
                this.initialize(name, factoryName, "fxClientDestinationUID=D." + topicName, ldapUserName, encryptedQueuePassword);
            } else {
                try {
                    //Will throw an exception if producer is closed
                    producer.getTimeToLive();
                } catch (Exception ex) {
                    log.info("MDERetryQueue.send(), {} Producer is closed for messageID: {} uuid: {} trackItemNumber: {} ...initializing", name, messageID, uuid, trackItemNumber);
                    this.initialize(name, factoryName, "fxClientDestinationUID=D." + topicName, ldapUserName, encryptedQueuePassword);
                }
            }
            if ((producer != null) && (session != null)) {
                BytesMessage bmsg = createByteMessage(messageProperties, messagePayLoad);
                super.sendIt(name, factoryName, bmsg);
                sent = true;
            } else {
                log.warn("MDERetryQueue.send(), Message not published to queue: {} factory: {} messageID: {} uuid: {} trackItemNumber: {}", queueName, factoryName, messageID, uuid, trackItemNumber);
            }
        } catch (Exception ex) {
            String message = null;
            if (!ex.getMessage().contains("Queue limit exceeded")) {
                log.error("MDERetryQueue.send(), {} Exception:", ex);
                message = "Exception sending to " + topicName;
            } else {
                log.error("MDERetryQueue.send(), {} Queue limit exceeded sending to XLT. messageID: {} uuid: {} trackItemNumber: {}", messageID, uuid, trackItemNumber);
                message = "Queue limit exceeded sending to " + topicName;
            }
            sendMSTeamAlert("Error Publishing to XLTNormal", message);
        } finally {
            if (messageProperties.containsKey(ResourceDefineBase.RULES_FILE) && messageProperties.get(ResourceDefineBase.RULES_FILE) != null) {
                rulesfile = (String) messageProperties.get(ResourceDefineBase.RULES_FILE);
            }
            if (messageProperties.containsKey(ResourceDefineBase.STATE_TYPE) && messageProperties.get(ResourceDefineBase.STATE_TYPE) != null) {
                stateType = (String) messageProperties.get(ResourceDefineBase.STATE_TYPE);
            }
            if (sent) {
                JSONObject main = new JSONObject();
                main.put("queue", topicName);
                main.put("messageID", messageID);
                main.put("uuid", uuid);
                main.put("trackItemNumber", trackItemNumber);
                main.put("rulesfile", rulesfile);
                main.put("stateType", stateType);
                String header = getMsgProps(messageProperties);
                main.put("header", header);
                String body = new String(messagePayLoad);
                main.put("body", body);
                log.info("MDERetryQueue.send(), Published. messageID: {} factory: {}", messageID, factoryName, kv("data", main));
            }
        }
    }

}
