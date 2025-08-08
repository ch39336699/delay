package com.fedex.intl.cd.utils;

import cimxmlmsgs.xmllitetrigger.XLTMessage;
import com.fedex.intl.cd.configuration.AppConfigs;
import com.fedex.intl.cd.defines.ResourceDefineBase;
import com.fedex.intl.cd.model.DelayedMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Service;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Service
public class DelayedMessageStats {

    @Autowired()
    public AppConfigs appConfigs;

    @Autowired
    XMLLiteTriggerCoder xltCoder;

    private static final int MAX_SIZE = 15000;

    private static HashMap<String, DelayedMessage> messages = new HashMap<String, DelayedMessage>();

    public void enqueueMessage(byte[] msgPayload, @Headers Map<String, Object> headers, String uuid, String trackItemNumber) {
        XLTMessage xltMsg;
        try {
            DelayedMessage message = new DelayedMessage();
            JSONObject main = new JSONObject();
            try {
                xltMsg = xltCoder.decode(msgPayload);
            } catch (Exception ex) {
                log.error("DelayedMessageStats.enqueueMessage(), Exception:", ex);
                return;
            }

            if (messages.size() > MAX_SIZE) {
                messages.clear();
            }

            if (xltMsg != null) {
                main.put("oid", xltMsg.getKey().getOidNumber());
                message.setOid(xltMsg.getKey().getOidNumber());
                message.setUUID(uuid);
                message.setTrackItemNumber(trackItemNumber);
                main.put(ResourceDefineBase.UUID, uuid);
                main.put(ResourceDefineBase.TRACK_ITEM_NBR, trackItemNumber);
                message.setBrokerInTime(System.currentTimeMillis());
                main.put("brokerInTime", System.currentTimeMillis());
                // message.setOid();
                if (headers != null) {
                    try {
                        if (headers.containsKey(ResourceDefineBase.RULES_FILE)) {
                            message.setRulesFile((String) headers.get(ResourceDefineBase.RULES_FILE));
                            main.put("rulesProcFile", headers.get(ResourceDefineBase.RULES_FILE));
                        }
                    } catch (Exception ex) {
                        log.error("DelayedMessageStats.enqueueMessage(), Exception:", ex);
                    }
                    try {
                        if (headers.containsKey(ResourceDefineBase.TYPE)) {
                            message.setType((String) headers.get(ResourceDefineBase.TYPE));
                            main.put("type", headers.get(ResourceDefineBase.TYPE));
                        }
                    } catch (Exception ex) {
                        log.error("DelayedMessageStats.enqueueMessage(), Exception:", ex);
                    }
                    try {
                        if (headers.containsKey(ResourceDefineBase.AMQ_SCHEDULED_DELAY)) {
                            if (headers.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY) instanceof Long) {
                                message.setAMQ_SCHEDULED_DELAY(String.valueOf(headers.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY)));
                            } else {
                                message.setAMQ_SCHEDULED_DELAY((String) headers.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY));
                            }
                            main.put("AMQ_SCHEDULED_DELAY", headers.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY));
                        }
                    } catch (Exception ex) {
                        log.error("DelayedMessageStats.enqueueMessage(), Exception:", ex);
                    }
                    try {
                        if (headers.containsKey(ResourceDefineBase.MSG_ID)) {
                            if (headers.get(ResourceDefineBase.MSG_ID) instanceof Long) {
                                message.setMSG_ID(String.valueOf(headers.get(ResourceDefineBase.MSG_ID)));
                            } else {
                                message.setMSG_ID((String) headers.get(ResourceDefineBase.MSG_ID));
                            }
                            main.put("MSG_ID", headers.get(ResourceDefineBase.MSG_ID));
                        }
                    } catch (Exception ex) {
                        log.error("DelayedMessageStats.enqueueMessage(), Exception:", ex);
                    }
                    try {
                        if (headers.containsKey(ResourceDefineBase.EXTERNAL_PUB)) {
                            message.setExternalPub((String) headers.get(ResourceDefineBase.EXTERNAL_PUB));
                            main.put("externalPub", headers.get(ResourceDefineBase.EXTERNAL_PUB));
                        }
                    } catch (Exception ex) {
                        log.error("DelayedMessageStats.enqueueMessage(), Exception:", ex);
                    }
                }
                message.setRuleset(xltMsg.getRuleSetName());
                main.put("ruleset", xltMsg.getRuleSetName().trim());
                messages.put(uuid, message);
                log.info("EventLog", kv("Enqueue", main));
            } else {
                log.warn("DelayedMessageStats.enqueueMessage(), Error decoding msgPayload");
            }
            // log.info("DelayedMessageStats.enqueueMessage(), ", kv("body", main));
        } catch (Exception ex) {
            log.error("DelayedMessageStats.enqueueMessage(), Exception: ", ex);
        }
    }

    public void enqueueRetryMessage(@Headers Map<String, Object> headers, String uuid, String trackItemNumber) {
        try {
            DelayedMessage message = new DelayedMessage();
            JSONObject main = new JSONObject();
            message.setUUID(uuid);
            message.setTrackItemNumber(trackItemNumber);
            main.put(ResourceDefineBase.UUID, uuid);
            main.put(ResourceDefineBase.TRACK_ITEM_NBR, trackItemNumber);
            message.setBrokerInTime(System.currentTimeMillis());
            message.setTrackItemNumber(trackItemNumber);
            main.put("brokerInTime", System.currentTimeMillis());
            if (headers != null) {
                try {
                    if (headers.containsKey(ResourceDefineBase.EXTERNAL_PUB)) {
                        main.put("externalPub", headers.get(ResourceDefineBase.EXTERNAL_PUB));
                    }
                } catch (Exception ex) {
                    log.error("DelayedMessageStats.enqueueRetryMessage(), Exception:", ex);
                }
                try {
                    if (headers.containsKey(ResourceDefineBase.AMQ_SCHEDULED_DELAY)) {
                        if (headers.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY) instanceof Long) {
                            message.setAMQ_SCHEDULED_DELAY(String.valueOf(headers.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY)));
                        } else {
                            message.setAMQ_SCHEDULED_DELAY((String) headers.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY));
                        }
                        main.put("AMQ_SCHEDULED_DELAY", headers.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY));
                    }
                } catch (Exception ex) {
                    log.error("DelayedMessageStats.enqueueRetryMessage(), Exception:", ex);
                }
            }
            messages.put(uuid, message);
            log.info("EventLog", kv("Enqueue", main));
        } catch (Exception ex) {
            log.error("DelayedMessageStats.enqueueRetryMessage(), Exception: ", ex);
        }
    }

    public DelayedMessage dequeueMessage(ActiveMQBytesMessage msg) {
        int retryCount = 0;
        String trackItemNumber = "n/a";
        DelayedMessage message = null;
        Date resultdate = null;
        JSONObject main = new JSONObject();
        SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yy HH:mm:ss");
        String MSG_ID = null;
        try {
            if (!msg.getProperties().containsKey("MSG_ID") || msg.getProperties().get("MSG_ID") == null) {
                log.info("NO MSG ID");
            } else {
                MSG_ID = msg.getProperties().get("MSG_ID").toString();
            }

            main.put("brokerOutTime", System.currentTimeMillis());
            if (msg.getProperties() != null) {
                if ((msg.getProperties().containsKey(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT)) && (msg.getProperties().get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT).toString().trim().length() > 0)) {
                    retryCount = Integer.parseInt(msg.getProperties().get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT).toString());
                }
                main.put(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT, retryCount);
                if ((msg.getProperties().containsKey(ResourceDefineBase.TRACK_ITEM_NBR)) && (msg.getProperties().get(ResourceDefineBase.TRACK_ITEM_NBR).toString().trim().length() > 0)) {
                    trackItemNumber = msg.getProperties().get(ResourceDefineBase.TRACK_ITEM_NBR).toString();
                    main.put(ResourceDefineBase.TRACK_ITEM_NBR, trackItemNumber);
                }
                if (msg.getProperties().containsKey(ResourceDefineBase.UUID)) {
                    String uuid = null;
                    try {
                        UTF8Buffer temp = (UTF8Buffer) msg.getProperties().get(ResourceDefineBase.UUID);
                        uuid = temp.toString();
                    } catch (Exception ex) {
                        try {
                            UUID temp = (UUID) msg.getProperties().get(ResourceDefineBase.UUID);
                            uuid = temp.toString();
                        } catch (Exception e) {
                            try {
                                uuid = (String) msg.getProperties().get(ResourceDefineBase.UUID);
                            } catch (Exception e2) {
                            }
                        }
                    }

                    if ((messages != null) && (messages.size() > 0) || (uuid != null) || (messages.containsKey(uuid))) {
                        message = messages.get(uuid);
                        messages.remove(uuid);
                        if (message == null) {
                            message = new DelayedMessage();
                            UUID newUUID = UUID.randomUUID();
                            uuid = newUUID.toString();
                            message.setUUID(uuid);
                            message.setTrackItemNumber(trackItemNumber);
                        }
                        message.setBrokerOutTime(System.currentTimeMillis());
                        main.put(ResourceDefineBase.UUID, uuid);
                        if (message.getBrokerInTime() > 0) {
                            resultdate = new Date(message.getBrokerInTime());
                            main.put("brokerInTime", sdf.format(resultdate));
                            main.put("timeQueued", message.getTimeQueued());
                        }
                        resultdate = new Date(message.getBrokerOutTime());
                        main.put("brokerOutTime", sdf.format(resultdate));
                        if (retryCount == 0) {
                            message.setDeliveryCnt(message.getDeliveryCnt() + 1);
                            main.put("deliveryCnt", message.getDeliveryCnt());
                        } else {
                            message.setDeliveryCnt(retryCount);
                            main.put("deliveryCnt", retryCount);
                        }

                        main.put("AMQ_SCHEDULED_DELAY", message.getAMQ_SCHEDULED_DELAY());
                        if (message.getMSG_ID() != null) {
                            main.put("MSG_ID", message.getMSG_ID());
                        } else {
                            main.put("MSG_ID", MSG_ID);
                            message.setMSG_ID(MSG_ID);
                        }
                        if (message.getExternalPub() != null) {
                            main.put("externalPub", message.getExternalPub());
                        }
                        main.put("oid", message.getOid());
                        main.put("rulesFile", message.getRulesFile());
                        main.put("ruleset", message.getRuleset());
                        if (messages.size() < 0) {
                            main.put("queueSize", 0);
                        } else {
                            main.put("queueSize", messages.size());
                        }
                        log.info("EventLog", kv("Dequeue", main));
                    } else {
                        message = new DelayedMessage();
                        message.setBrokerOutTime(System.currentTimeMillis());
                        UUID newUUID = UUID.randomUUID();
                        message.setBrokerOutTime(System.currentTimeMillis());
                        if (retryCount == 0) {
                            message.setDeliveryCnt(1);
                            main.put("deliveryCnt", 1);
                        } else {
                            message.setDeliveryCnt(retryCount);
                            main.put("deliveryCnt", retryCount);
                        }
                        message.setUUID(newUUID.toString());
                        message.setTrackItemNumber(trackItemNumber);
                        main.put(ResourceDefineBase.UUID, newUUID.toString());
                        main.put("brokerOutTime", sdf.format(resultdate));
                        main.put("MSG_ID", MSG_ID);
                        message.setMSG_ID(MSG_ID);
                        try {
                            byte[] messagePayLoad = getMessagePayload(msg);
                            XLTMessage xltMsg = xltCoder.decode(messagePayLoad);
                            message.setOid(xltMsg.getKey().getOidNumber());
                            main.put("oid", xltMsg.getKey().getOidNumber());
                            message.setRuleset(xltMsg.getRuleSetName());
                            main.put("ruleset", xltMsg.getRuleSetName());
                        } catch (Exception ex) {
                            log.error("DelayedMessageStats.dequeueMessage(), Exception:", ex);
                            throw ex;
                        }
                        if (messages.size() < 0) {
                            main.put("queueSize", 0);
                        } else {
                            main.put("queueSize", messages.size());
                        }
                        log.info("EventLog", kv("Dequeue", main));
                    }
                } else {
                    message = new DelayedMessage();
                    UUID uuid = UUID.randomUUID();
                    message.setBrokerOutTime(System.currentTimeMillis());
                    if (retryCount == 0) {
                        message.setDeliveryCnt(1);
                        main.put("deliveryCnt", 1);
                    } else {
                        message.setDeliveryCnt(retryCount);
                        main.put("deliveryCnt", retryCount);
                    }
                    message.setUUID(uuid.toString());
                    main.put("MSG_ID", MSG_ID);
                    message.setMSG_ID(MSG_ID);
                    main.put(ResourceDefineBase.UUID, uuid.toString());
                    message.setTrackItemNumber(trackItemNumber);
                    main.put("brokerOutTime", sdf.format(resultdate));
                    try {
                        byte[] messagePayLoad = getMessagePayload(msg);
                        XLTMessage xltMsg = xltCoder.decode(messagePayLoad);
                        message.setOid(xltMsg.getKey().getOidNumber());
                        main.put("oid", xltMsg.getKey().getOidNumber());
                        message.setRuleset(xltMsg.getRuleSetName());
                        main.put("ruleset", xltMsg.getRuleSetName());
                    } catch (Exception ex) {
                        log.error("DelayedMessageStats.dequeueMessage(), Exception:", ex);
                        throw ex;
                    }
                    if (messages.size() < 0) {
                        main.put("queueSize", 0);
                    } else {
                        main.put("queueSize", messages.size());
                    }
                    log.info("EventLog", kv("Dequeue", main));
                }
            } else {
                log.warn("DelayedMessageStats.dequeueMessage(), msg properties null or blank");
            }
        } catch (Exception ex) {
            log.error("DelayedMessageStats.dequeueMessage(), Exception: ", ex);
        }
        return message;
    }

    public DelayedMessage dequeueRetryMessage(ActiveMQBytesMessage msg, String externalPub) {
        int retryCount = 0;
        DelayedMessage message = null;
        SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yy HH:mm:ss");
        String trackItemNumber = "n/a";
        Date resultdate = null;
        JSONObject main = new JSONObject();

        try {
            main.put("brokerOutTime", System.currentTimeMillis());
            main.put("externalPub", externalPub);
            if (msg.getProperties() != null) {
                if ((msg.getProperties().containsKey(ResourceDefineBase.TRACK_ITEM_NBR)) && (msg.getProperties().get(ResourceDefineBase.TRACK_ITEM_NBR).toString().trim().length() > 0)) {
                    trackItemNumber = msg.getProperties().get(ResourceDefineBase.TRACK_ITEM_NBR).toString();
                    main.put(ResourceDefineBase.TRACK_ITEM_NBR, trackItemNumber);
                }
                if ((msg.getProperties().containsKey(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT)) && (msg.getProperties().get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT).toString().trim().length() > 0)) {
                    retryCount = Integer.parseInt(msg.getProperties().get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT).toString());
                }
                main.put(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT, retryCount);
                if (msg.getProperties().containsKey(ResourceDefineBase.UUID)) {
                    String uuid = null;
                    try {
                        UTF8Buffer temp = (UTF8Buffer) msg.getProperties().get(ResourceDefineBase.UUID);
                        uuid = temp.toString();
                    } catch (Exception ex) {
                        try {
                            UUID temp = (UUID) msg.getProperties().get(ResourceDefineBase.UUID);
                            uuid = temp.toString();
                        } catch (Exception e) {
                            try {
                                uuid = (String) msg.getProperties().get(ResourceDefineBase.UUID);
                            } catch (Exception e2) {
                            }
                        }
                    }
                    if ((messages != null) && (messages.size() > 0) || (uuid != null) || (messages.containsKey(uuid))) {
                        message = messages.get(uuid);
                        if (message == null) {
                            message = new DelayedMessage();
                            UUID newUUID = UUID.randomUUID();
                            uuid = newUUID.toString();
                            message.setUUID(uuid);
                            message.setTrackItemNumber(trackItemNumber);
                        }
                        message.setBrokerOutTime(System.currentTimeMillis());
                        main.put(ResourceDefineBase.UUID, uuid);
                        if (message.getBrokerInTime() > 0) {
                            resultdate = new Date(message.getBrokerInTime());
                            main.put("brokerInTime", sdf.format(resultdate));
                            main.put("timeQueued", message.getTimeQueued());
                        }
                        resultdate = new Date(message.getBrokerOutTime());
                        main.put("brokerOutTime", sdf.format(resultdate));
                        message.setDeliveryCnt(message.getDeliveryCnt() + 1);
                        main.put("deliveryCnt", message.getDeliveryCnt());
                        main.put("AMQ_SCHEDULED_DELAY", message.getAMQ_SCHEDULED_DELAY());
                        main.put("MSG_ID", message.getMSG_ID());
                        main.put("externalPub", message.getExternalPub());
                        messages.remove(uuid);
                        if (messages.size() < 0) {
                            main.put("queueSize", 0);
                        } else {
                            main.put("queueSize", messages.size());
                        }
                        log.info("EventLog", kv("Dequeue", main));
                    } else {
                        message = new DelayedMessage();
                        message.setBrokerOutTime(System.currentTimeMillis());
                        UUID newUUID = UUID.randomUUID();
                        message.setBrokerOutTime(System.currentTimeMillis());
                        message.setDeliveryCnt(1);
                        main.put("deliveryCnt", 1);
                        message.setUUID(newUUID.toString());
                        message.setTrackItemNumber(trackItemNumber);
                        main.put(ResourceDefineBase.UUID, newUUID.toString());
                        main.put("brokerOutTime", sdf.format(resultdate));
                        if (messages.size() < 0) {
                            main.put("queueSize", 0);
                        } else {
                            main.put("queueSize", messages.size());
                        }
                        log.info("EventLog", kv("Dequeue", main));
                    }
                } else {
                    message = new DelayedMessage();
                    UUID uuid = UUID.randomUUID();
                    message.setBrokerOutTime(System.currentTimeMillis());
                    message.setDeliveryCnt(1);
                    main.put("deliveryCnt", 1);
                    message.setUUID(uuid.toString());
                    message.setTrackItemNumber(trackItemNumber);
                    main.put(ResourceDefineBase.UUID, uuid.toString());
                    main.put("brokerOutTime", sdf.format(resultdate));
                    if (messages.size() < 0) {
                        main.put("queueSize", 0);
                    } else {
                        main.put("queueSize", messages.size());
                    }
                    log.info("EventLog", kv("Dequeue", main));
                }
            } else {
                log.warn("DelayedMessageStats.dequeueRetryMessage(), msg properties null or blank");
            }
        } catch (Exception ex) {
            log.error("DelayedMessageStats.dequeueRetryMessage(), Exception: ", ex);
        }
        return message;
    }

    public int enqueueCount() {
        int count = 0;
        try {
            if (messages.size() > 0) {
                for (String uuid : messages.keySet()) {
                    if (messages.get(uuid).getBrokerOutTime() == 0) {
                        count++;
                    }
                }
            }
        } catch (Exception ex) {
            log.error("DelayedMessageStats.enqueueCount(), Exception: ", ex);
        }
        return count;
    }

    public void printMessages() {
        JSONObject main = new JSONObject();
        try {
            if (messages.size() > 0) {
                for (String uuid : messages.keySet()) {
                    main.put(uuid, messages.get(uuid).toJSON());
                }
                log.info("DelayedMessageStats.printMessages(), ", kv("messages", main));
            } else {
                log.info("DelayedMessageStats.printMessages(), No Messages found");
            }
        } catch (Exception ex) {
            log.error("DelayedMessageStats.printMessages(), Exception: ", ex);
        }
    }

    protected byte[] getMessagePayload(Message inputMessage) throws Exception {

        if (inputMessage == null) {
            throw new Exception("Input message is null.  ");
        }
        byte[] msgPayload = null;

        try {
            if (inputMessage instanceof TextMessage) {
                String textMsg = ((TextMessage) inputMessage).getText();
                if (textMsg == null || textMsg.equals("")) {
                    throw new Exception("TextMessage is null or blank. ");
                }
                msgPayload = textMsg.getBytes();
            } else if (inputMessage instanceof BytesMessage) {
                BytesMessage byteMsg = (BytesMessage) inputMessage;
                if (byteMsg.getBodyLength() == 0) {
                    throw new Exception("byteMsg length is 0. ");
                }
                msgPayload = new byte[(int) byteMsg.getBodyLength()];
                byteMsg.readBytes(msgPayload);
            } else {
                String jmsType = "";
                try {
                    jmsType = inputMessage.getJMSType();
                } catch (Exception ex) {
                    jmsType = "unknown";
                }
                throw new Exception("Invalid JmsType: " + jmsType);
            }
        } catch (Exception ex) {
            throw ex;
        }
        return msgPayload;
    }
}
