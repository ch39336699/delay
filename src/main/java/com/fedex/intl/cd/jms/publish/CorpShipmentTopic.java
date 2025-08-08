package com.fedex.intl.cd.jms.publish;

import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import java.util.Map;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Service
public class CorpShipmentTopic extends PublisherBase {

    private final String name = "corpShipmentTopic";

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
            log.trace("CorpShipmentTopic.setup(), Called.");
            this.initialize(name, factoryName, "fxClientDestinationUID=D." + topicName, ldapUserName, encryptedQueuePassword);
        } catch (Exception ex) {
            log.error("CorpShipmentTopic.setup(), Exception:", ex);
        }
    }

    public void send(byte[] messagePayLoad, Map<String, Object> messageProperties, String uuid, String trackItemNumber) throws JMSException {
        boolean sent = false;
        try {
            if ((producer == null) || (session == null)) {
                log.warn("CorpShipmentTopic.send(), Producer or Session is null  for {} ...initializing", name);
                this.initialize(name, factoryName, "fxClientDestinationUID=D." + topicName, ldapUserName, encryptedQueuePassword);
            } else {
                try {
                    //Will throw an exception if producer is closed
                    producer.getTimeToLive();
                } catch (Exception ex) {
                    log.info("CorpShipmentTopic.send(byte[]), {} Producer is closed ...initializing", name);
                    this.initialize(name, factoryName, "fxClientDestinationUID=D." + topicName, ldapUserName, encryptedQueuePassword);
                }
            }
            if ((producer != null) && (session != null)) {
                BytesMessage bmsg = createByteMessage(messageProperties, messagePayLoad);
                super.sendIt(name, factoryName, bmsg);
                sent = true;
                publishedCnt++;
            } else {
                log.warn("CorpShipmentTopic.send(), Message not published to topic: {} factory: {} uuid: {} trackItemNumber: {}", topicName, factoryName, uuid, trackItemNumber);
                //sendMSTeamAlert(state, "Error Publishing to CorpShipment", "Producer or session null for " + topicName);
            }
        } catch (Exception ex) {
            String message = null;
            if (!ex.getMessage().contains("Queue limit exceeded")) {
                log.error("CorpShipmentTopic.send(byte[]), Exception:", ex);
                message = "Exception sending to " + topicName;
            } else {
                log.error("CorpShipmentTopic.send(byte[]), Queue limit exceeded sending to CorpShipment");
                message = "Queue limit exceeded sending to " + topicName;
            }
            //sendMSTeamAlert(state, "Error Publishing to CorpShipment", message);
        } finally {
            if (sent) {
                JSONObject main = new JSONObject();
                main.put("queue", topicName);
                main.put("uuid", uuid);
                main.put("trackItemNumber", trackItemNumber);
                String header = getMsgProps(messageProperties);
                main.put("header", header);
                String body = new String(messagePayLoad);
                main.put("body", body);
                log.info("CorpShipmentTopic.send(), Published. factory: {}", factoryName, kv("data", main));
            }
        }
    }

}
