package com.fedex.intl.cd.worker;

import com.fedex.intl.cd.configuration.AppConfigs;
import com.fedex.intl.cd.data.MsgId;
import com.fedex.intl.cd.defines.ResourceDefineBase;
import com.fedex.intl.cd.jms.publish.ErrorJMSPublisher;
import com.fedex.intl.cd.jms.publish.InternalPub;
import com.fedex.intl.cd.model.DelayedMessage;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Service class for Processing Messages from ActiveMQ
 * Code from com.fedex.intl.cd.bridgebean.BaseBridgeBean will go here
 */
@Slf4j
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public abstract class MessageProcessorBase {

    @Autowired
    @Qualifier("oracleDataSource")
    private DataSource dataSource;

    @Autowired()
    public AppConfigs appConfigs;

    @Autowired
    InternalPub internalPub;

    @Autowired
    ErrorJMSPublisher errorJMSPublisher;

    @Value("CIM_CORP")
    protected String SCHEMA_NAME;

    public int REPLAY_LIMIT = 5;

    public long REPLAY_DELAY_DEFAULT_MS = 2000;
    public long REPLAY_DELAY_FIRST_MS = 2000;
    public long REPLAY_DELAY_SECOND_MS = 10000;
    public long REPLAY_DELAY_THIRD_MS = 20000;
    public long REPLAY_DELAY_FORTH_MS = 30000;
    public long REPLAY_DELAY_FIFTH_MS = 40000;
    public long REPLAY_ERROR_DELAY_MS = 20000;
    boolean SEND_NO_MSG_ID_FOUND_TO_ERROR = false;
    public static String msgTypeForThisProcessor = "-not set yet-";

    protected abstract String getProcessorName();

    protected abstract void publisherSendMessage(byte[] messagePayLoad, @Headers Map<String, Object> headers, long messageID, String uuid, String trackItemNumber) throws Exception;

    protected abstract void publisherSendMessage(Message message, @Headers Map<String, Object> headers, long messageID, String uuid, String trackItemNumber) throws Exception;

    /**
     * Initialize MessageProcessorService
     * Any pre setup where when the Spring service starts
     */
    @PostConstruct
    public void setup() throws Exception {
        try {
            if (log.isDebugEnabled()) {
                log.debug("MessageProcessorBase.setup()");
            }
            TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
            msgTypeForThisProcessor = getProcessorName();
        } catch (Exception e) {
            log.error("MessageProcessorBase.setup(), Exception: {}", e);
            //assumed DB connections are closed also.
            throw e;
        }
    }

    /**
     * onMessage
     * messagePayload is from the ActiveMQ queue. Publisher is to the outbound JMS.
     */
    public boolean onMessageExternal(Message inputMessage, byte[] messagePayLoad, Map<String, Object> headers, DelayedMessage message) {
        String logInfo = "MessageProcessorBase.onMessageExternal(), ";
        String uuid = UUID.randomUUID().toString();
        String trackItemNumber = "n/a";
        String extPub = "XLT";
        java.sql.Connection dbConnection = null;
        StringBuffer msgStat = new StringBuffer();
        HashMap<String, Object> messageProperties = new HashMap<String, Object>();
        long messageIdFromProperties = 0;
        boolean messageDelayed = false;
        long before = System.currentTimeMillis();

        if (log.isTraceEnabled()) {
            log.trace(logInfo + "Started");
        }

        //************************************************************
        // Validate and Get Message Data
        //************************************************************
        readConfig();

        if (headers.containsKey(ResourceDefineBase.UUID) && (headers.get(ResourceDefineBase.UUID) != null)) {
            uuid = (String) headers.get(ResourceDefineBase.UUID);
        }
        if (headers.containsKey(ResourceDefineBase.TRACK_ITEM_NBR) && (headers.get(ResourceDefineBase.TRACK_ITEM_NBR) != null)) {
            trackItemNumber = (String) headers.get(ResourceDefineBase.TRACK_ITEM_NBR);
        }
        if (headers.containsKey(ResourceDefineBase.EXTERNAL_PUB) && (headers.get(ResourceDefineBase.EXTERNAL_PUB) != null)) {
            extPub = (String) headers.get(ResourceDefineBase.EXTERNAL_PUB);
        }
        try {
            if (headers.containsKey(ResourceDefineBase.MSG_ID) && (headers.get(ResourceDefineBase.MSG_ID) != null)) {
                messageIdFromProperties = Long.parseLong((String) headers.get(ResourceDefineBase.MSG_ID));
                if (messageIdFromProperties > 0) {
                    //MSG_ID found so need to do DB actions
                    logInfo = logInfo + "(msgID:" + messageIdFromProperties + " uuid: " + uuid + " trackItemNumber: " + trackItemNumber + ")";
                    try {
                        dbConnection = connectToDatabase();
                    } catch (Exception e) {
                        log.error(logInfo + " Failed the connectToDatabase. Retry the message: " + e, e);
                        msgStat.append("Failed the connectToDatabase, sent to error");
                        sendToErrorQueue(messagePayLoad, headers, ResourceDefineBase.RETRY_MESSAGE_RESET_COUNTER, messageIdFromProperties, uuid, trackItemNumber);
                        return false;
                    }
                    //Delete the MSG_ID from the table
                    int numberOfRowsDeleted = deleteFromMessageIdTable(messageIdFromProperties, dbConnection);

                    if (numberOfRowsDeleted == 0) {
                        //Record not there YET or have an issue!.. Retry X number of times.
                        if (log.isDebugEnabled()) {
                            if ((headers.containsKey(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT)) && (headers.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT).toString().trim().length() > 0)) {
                                int retryCount = Integer.parseInt(headers.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT).toString());
                                log.debug("{} No rows found in MSG_ID Table. MESSAGE_ID_RETRY_COUNT: {}", logInfo, retryCount);
                                msgStat.append(" No rows found in MSG_ID Table. MESSAGE_ID_RETRY_COUNT: " + retryCount);
                            } else {
                                log.debug("{} No rows found in MSG_ID Table. MESSAGE_ID_RETRY_COUNT: 0", logInfo);
                                msgStat.append(" No rows found in MSG_ID Table. MESSAGE_ID_RETRY_COUNT: 0");
                            }
                        }
                        try {
                            messageProperties = getMessageProperties(inputMessage);
                        } catch (Exception ex) {
                            log.error("MessageProcessorBase.onMessageExternal(), Exception. Keep going.", ex);
                        }
                        log.info("++++++++++++++++++++++++++++++++++ PUB INTERNAL {} ++++++++++++++++++++++++++++++++", uuid);
                        msgStat = putOnInternalQueueFirstTime(inputMessage, messagePayLoad, messageProperties, ResourceDefineBase.RETRY_REPLAY, messageIdFromProperties, uuid, trackItemNumber, msgStat);
                        messageDelayed = true;
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("{} {} Rows deleted in MSG_ID Table", logInfo, numberOfRowsDeleted);
                        }
                        msgStat.append(numberOfRowsDeleted + " rows deleted in MSG_ID, Publishing to " + extPub);
                        log.info("============================================ PUB EXTERNAL ============================================");
                        publisherSendMessage(messagePayLoad, headers, messageIdFromProperties, uuid, trackItemNumber);
                        if (dbConnection != null) {
                            dbConnection.commit();
                        }
                    }
                } else {
                    //MSG_ID found in properties but contains 0. Don't do any DB actions and just send to XLT
                    msgStat.append("MSG_ID found in properties but contains 0, Publishing to " + extPub);
                    publisherSendMessage(messagePayLoad, headers, messageIdFromProperties, uuid, trackItemNumber);
                }
            } else {
                //NO MSG_ID in properties so don't do any DB actions and just send to XLT
                msgStat.append("No MSG_ID in properties, Publishing to " + extPub);
                publisherSendMessage(messagePayLoad, headers, messageIdFromProperties, uuid, trackItemNumber);
            }
        } catch (Exception e) {
            log.error("MessageProcessorBase.onMessage(), Failed to get Message Properties Message ID, ignoring message.  " + logInfo, e);
            try {
                if (dbConnection != null) {
                    dbConnection.rollback();
                }
            } catch (Exception ex) {
                log.error(logInfo + "Failed to SEND message and Failed to rollback the delete.. " +
                        "MIGHT end up on error queue." + ex, ex);
                throw new RuntimeException("rollback to original queue");
                //Could we kill dbConnection and make new one?
                //TODO figure this out!  this did NOT rollback to original queue!
                //https://stackoverrun.com/fr/q/12132217
                //Are we allowed XA or not?   (WLS was not allowed)
            }
            sendToErrorQueue(messagePayLoad, headers, ResourceDefineBase.RETRY_MESSAGE_RESET_COUNTER, messageIdFromProperties, uuid, trackItemNumber);
            msgStat.append("Exception in XLT onMessage, sent to error queue");
            return false;
        } finally {
            closeConnectionToDatabase(messageIdFromProperties, dbConnection);
            printEventLog(headers, message, msgStat.toString(), messageDelayed);
        }
        // if we're still here, it's happy flow, log a happy message
        long totalTime = System.currentTimeMillis() - before;
        if (log.isDebugEnabled()) {
            log.debug("{} successfully sent...took: {} milliSeconds", logInfo, totalTime);
        }
        return true;
    }

    /**
     * onMessage
     * messagePayload is from the ActiveMQ queue. Publisher is to the outbound JMS.
     */
    public boolean onMessageInternal(Message inputMessage, HashMap<String, Object> messageProperties, DelayedMessage message) {
        String logInfo = "MessageProcessorBase.onMessageInternal(), ";
        String uuid = "";
        String trackItemNumber = "n/a";
        String extPub = "XLT";
        java.sql.Connection dbConnection = null;
        boolean duplicate = false;
        StringBuffer msgStat = new StringBuffer();
        long messageIdFromProperties = 0;
        long before = System.currentTimeMillis();

        if (log.isTraceEnabled()) {
            log.trace(logInfo + "Started");
        }

        //************************************************************
        // Validate and Get Message Data
        //************************************************************
        byte[] messagePayLoad;
        readConfig();
        try {
            messagePayLoad = getMessagePayload(inputMessage);
        } catch (Exception ex) {
            log.error("MessageProcessorBase.onMessageInternal(), Exception.", ex);
            return false;
        }

        if (messageProperties.containsKey(ResourceDefineBase.UUID) && (messageProperties.get(ResourceDefineBase.UUID) != null)) {
            uuid = (String) messageProperties.get(ResourceDefineBase.UUID);
        }
        if (messageProperties.containsKey(ResourceDefineBase.TRACK_ITEM_NBR) && (messageProperties.get(ResourceDefineBase.TRACK_ITEM_NBR) != null)) {
            trackItemNumber = (String) messageProperties.get(ResourceDefineBase.TRACK_ITEM_NBR);
        }
        if (messageProperties.containsKey(ResourceDefineBase.EXTERNAL_PUB) && (messageProperties.get(ResourceDefineBase.EXTERNAL_PUB) != null)) {
            extPub = (String) messageProperties.get(ResourceDefineBase.EXTERNAL_PUB);
        }
        try {
            if (messageProperties.containsKey(ResourceDefineBase.MSG_ID) && (messageProperties.get(ResourceDefineBase.MSG_ID) != null)) {
                messageIdFromProperties = Long.parseLong((String) messageProperties.get(ResourceDefineBase.MSG_ID));
                if (messageIdFromProperties > 0) {
                    //MSG_ID found so need to do DB actions
                    logInfo = logInfo + "(msgID:" + messageIdFromProperties + " uuid: " + uuid + " trackItemNumber: " + trackItemNumber + ")";
                    try {
                        dbConnection = connectToDatabase();
                    } catch (Exception e) {
                        log.error(logInfo + " Failed the connectToDatabase. Retry the message: " + e, e);
                        msgStat.append("Failed the connectToDatabase, Sent to error");
                        sendToErrorQueue(messagePayLoad, messageProperties, ResourceDefineBase.RETRY_MESSAGE_RESET_COUNTER, messageIdFromProperties, uuid, trackItemNumber);
                        return false;
                    }
                    //Delete the MSG_ID from the table
                    int numberOfRowsDeleted = deleteFromMessageIdTable(messageIdFromProperties, dbConnection);
                    if (numberOfRowsDeleted == 0) {
                        //Record not there YET or have an issue!.. Retry X number of times.
                        if (log.isDebugEnabled()) {
                            if ((messageProperties.containsKey(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT)) && (messageProperties.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT).toString().trim().length() > 0)) {
                                int retryCount = Integer.parseInt(messageProperties.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT).toString());
                                log.debug("{} No rows found in MSG_ID Table. MESSAGE_ID_RETRY_COUNT: {}", logInfo, retryCount);
                                msgStat.append(" No rows found in MSG_ID Table. MESSAGE_ID_RETRY_COUNT: " + retryCount);
                            } else {
                                log.debug("{} No rows found in MSG_ID Table. MESSAGE_ID_RETRY_COUNT: 0", logInfo);
                                msgStat.append(" No rows found in MSG_ID Table. MESSAGE_ID_RETRY_COUNT: 0");
                            }
                        }
                        if((message.getOid() > 0) && (message.getBrokerInTime() > 0)) {
                            msgStat = putOnInternalQueue(inputMessage, messagePayLoad, messageProperties, ResourceDefineBase.RETRY_REPLAY, messageIdFromProperties, uuid, trackItemNumber, msgStat);
                        } else {
                            duplicate = true;
                        }
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("{} {} Rows deleted in MSG_ID Table", logInfo, numberOfRowsDeleted);
                        }
                        msgStat.append(numberOfRowsDeleted + " rows deleted in MSG_ID, Publishing to " + extPub);
                        publisherSendMessage(messagePayLoad, messageProperties, messageIdFromProperties, uuid, trackItemNumber);
                        if (dbConnection != null) {
                            dbConnection.commit();
                        }
                    }
                } else {
                    //MSG_ID found in properties but contains 0. Don't do any DB actions and just send to XLT
                    msgStat.append("MSG_ID found in properties but contains 0, Publishing to " + extPub);
                    publisherSendMessage(messagePayLoad, messageProperties, messageIdFromProperties, uuid, trackItemNumber);
                }
            } else {
                //NO MSG_ID in properties so don't do any DB actions and just send to XLT
                msgStat.append("No MSG_ID in properties, Publishing to " + extPub);
                publisherSendMessage(messagePayLoad, messageProperties, messageIdFromProperties, uuid, trackItemNumber);
            }
        } catch (Exception e) {
            log.error("MessageProcessorBase.onMessageInternal(), Failed to get Message Properties Message ID, ignoring message.  " + logInfo, e);
            try {
                if (dbConnection != null) {
                    dbConnection.rollback();
                }
            } catch (Exception ex) {
                log.error(logInfo + "Failed to SEND message and Failed to rollback the delete.. " +
                        "MIGHT end up on error queue." + ex, ex);
                throw new RuntimeException("rollback to original queue");
                //https://stackoverrun.com/fr/q/12132217
            }
            sendToErrorQueue(messagePayLoad, messageProperties, ResourceDefineBase.RETRY_MESSAGE_RESET_COUNTER, messageIdFromProperties, uuid, trackItemNumber);
            msgStat.append("Exception in XLT onMessage, sent to error queue");
            return false;
        } finally {
            closeConnectionToDatabase(messageIdFromProperties, dbConnection);
            if(!duplicate) {
                printEventLog(messageProperties, message, msgStat.toString(), true);
            }
        }
        // if we're still here, it's happy flow, log a happy message
        long totalTime = System.currentTimeMillis() - before;
        if (log.isDebugEnabled()) {
            log.debug("{} successfully sent...took: {} milliSeconds", logInfo, totalTime);
        }
        return true;
    }


    /**
     * Method to process a message-send failure by putting the message back on the JMS queue with a delay
     * in order to retry it. MSG_ID failures are limited to the number of retries allowed. EIT put failures retry forever.
     *
     * @param inputMessage      - the complete input message
     * @param messageProperties - the message properties extracted from the inputMessage
     * @param handleEITError    - boolean. if true - handle as an EIT (output) failure, false - handle as a MSG_ID (input) failure.
     */
    private StringBuffer putOnInternalQueueFirstTime(Message inputMessage, byte[] messagePayLoad, HashMap<String, Object> messageProperties, boolean handleEITError, long messageIdFromProperties, String uuid, String trackItemNumber, StringBuffer msgStat) {
        String logInfo = "MessageProcessorBase.putOnInternalQueueFirstTime(msgID:" + messageIdFromProperties + " uuid: " + uuid + " trackItemNumber: " + trackItemNumber + " ), ";
        int messageIdReplayCount = 0;
        String rulesfile = "";
        String stateType = "";
        readConfig();
        try {
            if (messageProperties.containsKey(ResourceDefineBase.RULES_FILE) && messageProperties.get(ResourceDefineBase.RULES_FILE) != null) {
                rulesfile = (String) messageProperties.get(ResourceDefineBase.RULES_FILE);
            }
            if (messageProperties.containsKey(ResourceDefineBase.STATE_TYPE) && messageProperties.get(ResourceDefineBase.STATE_TYPE) != null) {
                stateType = (String) messageProperties.get(ResourceDefineBase.STATE_TYPE);
            }
            if ((messageProperties.containsKey(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT)) && (messageProperties.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT) != null)) {
                messageIdReplayCount = Integer.parseInt((String) messageProperties.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT));
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug(" {} Missing or non-Long MESSAGE_ID_RETRY_COUNT value in messageProperties. STATE_TYPE {}, RULES_FILE {}", logInfo, stateType, rulesfile);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("{} MESSAGE_ID_RETRY_COUNT: {}, REPLAY_LIMIT: {}, STATE_TYPE {}, RULES_FILE {}", logInfo, messageIdReplayCount, REPLAY_LIMIT, stateType, rulesfile);
        }

        //Unless this is an EIT error, if replays have exceeded the replayLimit, then move to error queue
        if (!handleEITError && messageIdReplayCount >= REPLAY_LIMIT) {
            if (log.isDebugEnabled()) {
                log.debug("{} REPLAY_LIMIT: {} reached. Sending to error queue. STATE_TYPE {}, RULES_FILE {}", logInfo, REPLAY_LIMIT, stateType, rulesfile);
            }
            try {
                if (SEND_NO_MSG_ID_FOUND_TO_ERROR) {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Retry count reached. Sending to error queue.. MESSAGE_ID_RETRY_COUNT: {}, REPLAY_LIMIT: {}, STATE_TYPE {}, RULES_FILE {}", logInfo, messageIdReplayCount, REPLAY_LIMIT, stateType, rulesfile);
                    }
                    msgStat.append(" Replays have exceeded the replayLimit, sending to error queue");
                    sendToErrorQueue(messagePayLoad, messageProperties, ResourceDefineBase.RETRY_MESSAGE_RESET_COUNTER, messageIdFromProperties, uuid, trackItemNumber);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Retry count reached. SEND_NO_MSG_ID_FOUND_TO_ERROR false. Not sending to error queue. MESSAGE_ID_RETRY_COUNT: {}, REPLAY_LIMIT: {}, STATE_TYPE {}, RULES_FILE {}", logInfo, messageIdReplayCount, REPLAY_LIMIT, stateType, rulesfile);
                    }
                    msgStat.append(" Replays have exceeded the replayLimit, SEND_NO_MSG_ID_FOUND_TO_ERROR false. Not sending to error queue");
                }
                return msgStat;
            } catch (Exception e) {
                log.error(logInfo + "Failed error queue.  Exception:" + e, e);
                //Now we must handle EIT error
                handleEITError = true;
            }
        }

        //If EIT error, reset the replay counter.
        if (handleEITError) {
            messageIdReplayCount = 0;
            // replayDelayLocal = EITERR_DELAY_SECS_MS;
        }
        //Increment retry counter and save it in the messageProperties
        messageIdReplayCount++;
        String tmpS = Integer.toString(messageIdReplayCount);
        messageProperties.put(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT, messageIdReplayCount);
        //Repackage MessageProperties (java says it's read-only, so we have to clear it and rebuild it)
        try {
            inputMessage.clearProperties();
            messageProperties.put(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT, "1");
            inputMessage = setMessageProperties(inputMessage, messageProperties);
        } catch (Exception e) {
            log.error(logInfo + "Exception setting properties " + e, e);
            msgStat.append(" Exception setting properties");
            return msgStat;
        }
        try {
            if (messageIdReplayCount > REPLAY_LIMIT) {
                if (SEND_NO_MSG_ID_FOUND_TO_ERROR) {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Retry count reached. Sending to error queue.. MESSAGE_ID_RETRY_COUNT: {}, REPLAY_LIMIT: {}, STATE_TYPE {}, RULES_FILE {}", logInfo, messageIdReplayCount, REPLAY_LIMIT, stateType, rulesfile);
                    }
                    msgStat.append(" Replays have exceeded the replayLimit, sending to error queue");
                    sendToErrorQueue(messagePayLoad, messageProperties, ResourceDefineBase.RETRY_MESSAGE_RESET_COUNTER, messageIdFromProperties, uuid, trackItemNumber);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Retry count reached. SEND_NO_MSG_ID_FOUND_TO_ERROR false. Not sending to error queue. MESSAGE_ID_RETRY_COUNT: {}, REPLAY_LIMIT: {}, STATE_TYPE {}, RULES_FILE {}", logInfo, messageIdReplayCount, REPLAY_LIMIT, stateType, rulesfile);
                    }
                    msgStat.append(" Replays have exceeded the replayLimit, SEND_NO_MSG_ID_FOUND_TO_ERROR false. Not sending to error queue");
                }
            } else {
                messageProperties.put(ResourceDefineBase.AMQ_SCHEDULED_DELAY, REPLAY_DELAY_FIRST_MS);
                internalPub.sendMessage(messagePayLoad, messageProperties, uuid, trackItemNumber);
                msgStat.append(" Publishing to internal queue first time,");
            }
        } catch (Exception e) {
            log.error(logInfo + "failed to publish to internal queue. Moving to error queue. " + e, e);
            try {
                //If we ever fail on putting back on the JMS queue, then move the message to error queue (rather than drop it on the floor)
                sendToErrorQueue(messagePayLoad, messageProperties, ResourceDefineBase.RETRY_MESSAGE_RESET_COUNTER, messageIdFromProperties, uuid, trackItemNumber);
            } catch (Exception e1) {
                log.error(logInfo + "(1): failure in error queue" + e1, e1);
            } // we've done everything we can to save that message, but...
            return msgStat;
        }
        if (log.isTraceEnabled()) {
            log.trace(logInfo + "completed");
        }
        return msgStat;
    }


    /**
     * Method to process a message-send failure by putting the message back on the JMS queue with a delay
     * in order to retry it. MSG_ID failures are limited to the number of retries allowed. EIT put failures retry forever.
     *
     * @param inputMessage      - the complete input message
     * @param messageProperties - the message properties extracted from the inputMessage
     * @param handleEITError    - boolean. if true - handle as an EIT (output) failure, false - handle as a MSG_ID (input) failure.
     */
    private StringBuffer putOnInternalQueue(Message inputMessage, byte[] messagePayLoad, HashMap<String, Object> messageProperties, boolean handleEITError, long messageIdFromProperties, String uuid, String trackItemNumber, StringBuffer msgStat) {
        String logInfo = "MessageProcessorBase.putOnInternalQueue(msgID:" + messageIdFromProperties + " uuid: " + uuid + " trackItemNumber: " + trackItemNumber + " ), ";
        int messageIdReplayCount = 0;
        String rulesfile = "";
        String stateType = "";
        readConfig();
        try {
            if (messageProperties.containsKey(ResourceDefineBase.RULES_FILE) && messageProperties.get(ResourceDefineBase.RULES_FILE) != null) {
                rulesfile = (String) messageProperties.get(ResourceDefineBase.RULES_FILE);
            }
            if (messageProperties.containsKey(ResourceDefineBase.STATE_TYPE) && messageProperties.get(ResourceDefineBase.STATE_TYPE) != null) {
                stateType = (String) messageProperties.get(ResourceDefineBase.STATE_TYPE);
            }
            if ((messageProperties.containsKey(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT)) && (messageProperties.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT) != null)) {
                messageIdReplayCount = Integer.parseInt((String) messageProperties.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT));
                if (messageIdReplayCount > 1) {
                    log.info("messageIdReplayCount > 1");
                }
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug(" {} Missing or non-Long MESSAGE_ID_RETRY_COUNT value in messageProperties. STATE_TYPE {}, RULES_FILE {}", logInfo, stateType, rulesfile);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("{} MESSAGE_ID_RETRY_COUNT: {}, REPLAY_LIMIT: {}, STATE_TYPE {}, RULES_FILE {}", logInfo, messageIdReplayCount, REPLAY_LIMIT, stateType, rulesfile);
        }

        //Unless this is an EIT error, if replays have exceeded the replayLimit, then move to error queue
        if (!handleEITError && messageIdReplayCount >= REPLAY_LIMIT) {
            if (log.isDebugEnabled()) {
                log.debug("{} REPLAY_LIMIT: {} reached. Sending to error queue. STATE_TYPE {}, RULES_FILE {}", logInfo, REPLAY_LIMIT, stateType, rulesfile);
            }
            try {
                if (SEND_NO_MSG_ID_FOUND_TO_ERROR) {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Retry count reached. Sending to error queue.. MESSAGE_ID_RETRY_COUNT: {}, REPLAY_LIMIT: {}, STATE_TYPE {}, RULES_FILE {}", logInfo, messageIdReplayCount, REPLAY_LIMIT, stateType, rulesfile);
                    }
                    msgStat.append(" Replays have exceeded the replayLimit, sending to error queue");
                    sendToErrorQueue(messagePayLoad, messageProperties, ResourceDefineBase.RETRY_MESSAGE_RESET_COUNTER, messageIdFromProperties, uuid, trackItemNumber);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Retry count reached. SEND_NO_MSG_ID_FOUND_TO_ERROR false. Not sending to error queue. MESSAGE_ID_RETRY_COUNT: {}, REPLAY_LIMIT: {}, STATE_TYPE {}, RULES_FILE {}", logInfo, messageIdReplayCount, REPLAY_LIMIT, stateType, rulesfile);
                    }
                    msgStat.append(" Replays have exceeded the replayLimit, SEND_NO_MSG_ID_FOUND_TO_ERROR false. Not sending to error queue");
                }
                return msgStat;
            } catch (Exception e) {
                log.error(logInfo + "Failed error queue.  Exception:" + e, e);
                //Now we must handle EIT error
                handleEITError = true;
            }
        }

        //If EIT error, reset the replay counter.
        if (handleEITError) {
            messageIdReplayCount = 0;
            // replayDelayLocal = EITERR_DELAY_SECS_MS;
        }
        //Increment retry counter and save it in the messageProperties
        messageIdReplayCount++;
        String tmpS = Integer.toString(messageIdReplayCount);

        //Repackage MessageProperties (java says it's read-only, so we have to clear it and rebuild it)
        try {
            inputMessage.clearProperties();
            messageProperties.put(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT, tmpS);
            messageProperties.put(ResourceDefineBase.AMQ_SCHEDULED_DELAY, getRelayTime(messageIdReplayCount));
            inputMessage = setMessageProperties(inputMessage, messageProperties);
        } catch (Exception e) {
            log.error(logInfo + "Exception setting properties " + e, e);
            msgStat.append(" Exception setting properties");
            return msgStat;
        }
        try {
            if (messageIdReplayCount >= REPLAY_LIMIT) {
                if (SEND_NO_MSG_ID_FOUND_TO_ERROR) {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Retry count reached. Sending to error queue.. MESSAGE_ID_RETRY_COUNT: {}, REPLAY_LIMIT: {}, STATE_TYPE {}, RULES_FILE {}", logInfo, messageIdReplayCount, REPLAY_LIMIT, stateType, rulesfile);
                    }
                    msgStat.append(" Replays have exceeded the replayLimit, sending to error queue");
                    sendToErrorQueue(messagePayLoad, messageProperties, ResourceDefineBase.RETRY_MESSAGE_RESET_COUNTER, messageIdFromProperties, uuid, trackItemNumber);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Retry count reached. SEND_NO_MSG_ID_FOUND_TO_ERROR false. Not sending to error queue. MESSAGE_ID_RETRY_COUNT: {}, REPLAY_LIMIT: {}, STATE_TYPE {}, RULES_FILE {}", logInfo, messageIdReplayCount, REPLAY_LIMIT, stateType, rulesfile);
                    }
                    msgStat.append(" Replays have exceeded the replayLimit, SEND_NO_MSG_ID_FOUND_TO_ERROR false. Not sending to error queue");
                }
            } else {
                internalPub.sendMessage(messagePayLoad, messageProperties, uuid, trackItemNumber);
                msgStat.append(" Republishing to internal queue,");
            }
        } catch (Exception e) {
            log.error(logInfo + "failed to publish to internal queue. Moving to error queue. " + e, e);
            try {
                //If we ever fail on putting back on the JMS queue, then move the message to error queue (rather than drop it on the floor)
                sendToErrorQueue(messagePayLoad, messageProperties, ResourceDefineBase.RETRY_MESSAGE_RESET_COUNTER, messageIdFromProperties, uuid, trackItemNumber);
            } catch (Exception e1) {
                log.error(logInfo + "(1): failure in error queue" + e1, e1);
            } // we've done everything we can to save that message, but...
            return msgStat;
        }
        if (log.isTraceEnabled()) {
            log.trace(logInfo + "completed");
        }
        return msgStat;
    }

    private long getRelayTime(int relayCycle) {
        try {
            switch (relayCycle) {
                case 1:
                    return REPLAY_DELAY_FIRST_MS;
                case 2:
                    return REPLAY_DELAY_SECOND_MS;
                case 3:
                    return REPLAY_DELAY_THIRD_MS;
                case 4:
                    return REPLAY_DELAY_FORTH_MS;
                case 5:
                    return REPLAY_DELAY_FIFTH_MS;
                default:
                    return REPLAY_DELAY_DEFAULT_MS;
            }
        } catch (Exception e) {
            log.error("MessageProcessorBase.getRelayTime(), " + e, e);
        }
        return REPLAY_DELAY_DEFAULT_MS;
    }

    /**
     * Makes sure our message is a valid message.
     */
    protected byte[] getMessagePayload(Message inputMessage) throws Exception {

        if (inputMessage == null) {
            throw new Exception("MessageProcessorBase.getMessagePayload(), Input message is null.  ");
        }

        byte[] msgPayload = null;

        try {
            if (inputMessage instanceof TextMessage) {
                String textMsg = ((TextMessage) inputMessage).getText();
                if (textMsg == null || textMsg.equals("")) {
                    throw new Exception("MessageProcessorBase.getMessagePayload(), TextMessage is null or blank. ");
                }
                msgPayload = textMsg.getBytes();
            } else if (inputMessage instanceof BytesMessage) {
                BytesMessage byteMsg = (BytesMessage) inputMessage;
                if (byteMsg.getBodyLength() == 0) {
                    throw new Exception("MessageProcessorBase.getMessagePayload(), byteMsg length is 0. ");
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
                throw new Exception("MessageProcessorBase.getMessagePayload(), Invalid JmsType: " + jmsType);
            }
        } catch (Exception ex) {
            throw ex;
        }
        return msgPayload;
    }

    /**
     * get the Message Properties
     */
    protected HashMap<String, Object> getMessageProperties(Message inputMessage) throws Exception {
        HashMap<String, Object> msgProperties = new HashMap<String, Object>();
        Enumeration<String> list = inputMessage.getPropertyNames();
        while (list.hasMoreElements()) {
            String key = list.nextElement();
            msgProperties.put(key, inputMessage.getStringProperty(key));
            if (log.isTraceEnabled()) {
                log.trace("MessageProcessorBase.onMessage(), msg properties: {} = {}", key, inputMessage.getStringProperty(key));
            }
        }
        return msgProperties;
    }


    /**
     * Method to process a message-send failure by putting the message back on the JMS queue with a delay
     * in order to retry it. MSG_ID failures are limited to the number of retries allowed. EIT put failures retry forever.
     *
     * @param messagePayLoad    - the message payload
     * @param messageProperties - the message properties extracted from the inputMessage
     * @param handleEITError    - boolean. if true - handle as an EIT (output) failure, false - handle as a MSG_ID (input) failure.
     */
    private void sendToErrorQueue(byte[] messagePayLoad, Map<String, Object> messageProperties, boolean handleEITError, long messageID, String uuid, String trackItemNumber) {
        String logInfo = "MessageProcessorBase.sendToErrorQueue(msgID:" + messageID + " uuid: " + uuid + " trackItemNumber: " + trackItemNumber + ") ";
        readConfig();
        long replayDelayLocal = REPLAY_ERROR_DELAY_MS;
        int messageIdReplayCount = 0;
        int amqDelay = 0;
        String rulesfile = "";
        String stateType = "";
        try {
            if ((messageProperties.containsKey(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT)) && messageProperties.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT) != null) {
                messageIdReplayCount = Integer.parseInt((String) messageProperties.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT));
            }
            if ((messageProperties.containsKey(ResourceDefineBase.AMQ_SCHEDULED_DELAY)) && messageProperties.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY) != null) {
                amqDelay = Integer.parseInt((String) messageProperties.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY));
            }
            if (messageProperties.containsKey(ResourceDefineBase.RULES_FILE) && messageProperties.get(ResourceDefineBase.RULES_FILE) != null) {
                rulesfile = (String) messageProperties.get(ResourceDefineBase.RULES_FILE);
            }
            if (messageProperties.containsKey(ResourceDefineBase.STATE_TYPE) && messageProperties.get(ResourceDefineBase.STATE_TYPE) != null) {
                stateType = (String) messageProperties.get(ResourceDefineBase.STATE_TYPE);
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("{} Missing or non-Long MESSAGE_ID_RETRY_COUNT value in messageProperties, STATE_TYPE {}, RULES_FILE {}", logInfo, stateType, rulesfile);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("{} MESSAGE_ID_RETRY_COUNT: {}, REPLAY_LIMIT: {}, STATE_TYPE {}, RULES_FILE {}", logInfo, messageIdReplayCount, REPLAY_LIMIT, stateType, rulesfile);
        }

        // Unless this is an EIT error, if replays have exceeded the replayLimit, then move to error queue
        if (!handleEITError && messageIdReplayCount >= REPLAY_LIMIT) {
            if (log.isDebugEnabled()) {
                log.debug("{} REPLAY_LIMIT: {} reached. Sending to error queue. STATE_TYPE {}, RULES_FILE {}", logInfo, REPLAY_LIMIT, stateType, rulesfile);
            }
            try {
                errorJMSPublisher.send(messagePayLoad, messageProperties, messageID);
                return;
            } catch (Exception e) {
                log.error(logInfo + "Failed publishing to error queue.  Proceed to handle EIT ERROR:" + e, e);
                //Now we must handle EIT error by.
                //We could not put to the error queue, so put to the Active MQ for replay
                handleEITError = true;
            }
        }

        // if EIT error, reset the replay counter.
        if (handleEITError) {
            messageIdReplayCount = 0;
            replayDelayLocal = REPLAY_ERROR_DELAY_MS;
        }

        Map<String, Object> newProperties = new HashMap<>();
        try {
            //Increment retry counter and save it in the messageProperties
            messageIdReplayCount++;
            String tmpS = Integer.toString(messageIdReplayCount);
            //Need to assign to new properties since adding new key will throw an exception that the properties is immutable
            newProperties = createMessagePropertiesFromExisting(messageProperties);
            newProperties.put(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT, tmpS);

            //Increment delay: 1st retry: 10 secs, 2nd: 20 secs, 3rd: 30 secs
            amqDelay = (int) (replayDelayLocal * messageIdReplayCount);
            newProperties.put(ResourceDefineBase.AMQ_SCHEDULED_DELAY, amqDelay);
        } catch (Exception e) {
            log.warn(logInfo + "failed to set StringProperty of retry counter back into inputMessage. " + e, e);
            //Keep going. Don't want to lose the message. Not critical that property not incremented
        }

        try {
            if (log.isDebugEnabled()) {
                log.debug(logInfo + " Putting back on Internal ActiveMQ. MESSAGE_ID_RETRY_COUNT: {}, AMQ_SCHEDULED_DELAY: {} STATE_TYPE {}, RULES_FILE {}", messageIdReplayCount, amqDelay, stateType, rulesfile);
            }
            internalPub.sendMessage(messagePayLoad, messageProperties, uuid, trackItemNumber);
        } catch (Exception e) {
            log.error(logInfo + " Failure in publishing to internal queue.  Attempting to ROLLBACK the MESSAGE" + e, e);
            throw new RuntimeException(e);
        }

        if (log.isTraceEnabled()) {
            log.trace(logInfo + "completed");
        }
    }

    private int deleteFromMessageIdTable(long messageIdFromProperties, java.sql.Connection dbConnection) throws Exception {
        String logInfo = msgTypeForThisProcessor + ".deleteFromMessageIdTable(msgID:" + messageIdFromProperties + ") ";

        MsgId inData = new MsgId();
        inData.setMsgId(messageIdFromProperties);
        PreparedStatement statement = null;
        int rowCount = 0;
        String deleteSQL = " DELETE FROM " + SCHEMA_NAME + ".MSG_ID  WHERE  MSG_ID = ? ";

        try {
            statement = dbConnection.prepareStatement(deleteSQL);
        } catch (SQLException ex3) {  // java.sql.SQLException: Connection has already been closed.
            log.warn(logInfo + "SQLException on PrepareStatement: " + ex3);
            return rowCount = 0;
        }
        try {
            statement.setLong(1, inData.getMsgId()); //MSG_ID
            rowCount = statement.executeUpdate();
        } catch (SQLException ex) {
            log.warn(logInfo + "SQLException: " + ex);
            throw ex;
        } catch (Exception ex2) {
            log.warn(logInfo + "Exception: " + ex2);
            throw ex2;
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
        if (log.isTraceEnabled()) {
            log.trace(logInfo + " complete ");
        }
        return rowCount;
    }

    protected void printEventLog(Map<String, Object> messageProperties, DelayedMessage message, String msgStat, boolean messageDelayed) {
        JSONObject eventLogObj = new JSONObject();
        SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yy HH:mm:ss");
        eventLogObj.put("brokerOutTime", sdf.format(System.currentTimeMillis()));
        if (message != null) {
            if (message.getBrokerInTime() > 0) {
                Date resultdate = new Date(message.getBrokerInTime());
                eventLogObj.put("brokerInTime", sdf.format(resultdate));
                eventLogObj.put("timeQueued", message.getTimeQueued());
            }
            eventLogObj.put("oid", message.getOid());
            if ((message.getAMQ_SCHEDULED_DELAY() != null) && (message.getAMQ_SCHEDULED_DELAY().trim().length() > 0)) {
                eventLogObj.put("AMQ_SCHEDULED_DELAY", message.getAMQ_SCHEDULED_DELAY());
            }
        }
        eventLogObj.put("msgStat", msgStat);
        eventLogObj.put("messageDelayed", messageDelayed);
        if (messageProperties != null) {
            if (messageProperties.containsKey(ResourceDefineBase.UUID) && (messageProperties.get(ResourceDefineBase.UUID) != null)) {
                eventLogObj.put("UUID", String.valueOf(messageProperties.get(ResourceDefineBase.UUID)));
            }
            if (messageProperties.containsKey(ResourceDefineBase.TRACK_ITEM_NBR) && (messageProperties.get(ResourceDefineBase.TRACK_ITEM_NBR) != null)) {
                eventLogObj.put("TRACK_ITEM_NBR", String.valueOf(messageProperties.get(ResourceDefineBase.TRACK_ITEM_NBR)));
            }
            if (messageProperties.containsKey(ResourceDefineBase.EXTERNAL_PUB) && (messageProperties.get(ResourceDefineBase.EXTERNAL_PUB) != null)) {
                eventLogObj.put("EXTERNAL_PUB", String.valueOf(messageProperties.get(ResourceDefineBase.EXTERNAL_PUB)));
            } else {
                eventLogObj.put("EXTERNAL_PUB", "XLT");
            }
            if (messageProperties.containsKey(ResourceDefineBase.RULES_FILE) && messageProperties.get(ResourceDefineBase.RULES_FILE) != null) {
                eventLogObj.put("RULES_FILE", String.valueOf(messageProperties.get(ResourceDefineBase.RULES_FILE)));
            }
            if (messageProperties.containsKey(ResourceDefineBase.STATE_TYPE) && messageProperties.get(ResourceDefineBase.STATE_TYPE) != null) {
                eventLogObj.put("STATE_TYPE", String.valueOf(messageProperties.get(ResourceDefineBase.STATE_TYPE)));
            }
            if ((messageProperties.containsKey(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT)) && (messageProperties.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT) != null)) {
                eventLogObj.put("MESSAGE_ID_RETRY_COUNT", String.valueOf(messageProperties.get(ResourceDefineBase.MESSAGE_ID_RETRY_COUNT)));
            } else {
                eventLogObj.put("MESSAGE_ID_RETRY_COUNT", "0");
            }
            if ((messageProperties.containsKey(ResourceDefineBase.AMQ_SCHEDULED_DELAY)) && (messageProperties.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY) != null)) {
                eventLogObj.put("AMQ_SCHEDULED_DELAY", String.valueOf(messageProperties.get(ResourceDefineBase.AMQ_SCHEDULED_DELAY)));
            }
            if ((messageProperties.containsKey(ResourceDefineBase.MSG_ID)) && (messageProperties.get(ResourceDefineBase.MSG_ID) != null)) {
                eventLogObj.put("MSG_ID", String.valueOf(messageProperties.get(ResourceDefineBase.MSG_ID)));
            }
        }

        try {
            log.info("EventLog", kv("EventLog", eventLogObj));
        } catch (Exception ex) {
            log.error("EventLogUtilV5.printEventLog(), Exception:", ex);
        }
    }


    private java.sql.Connection connectToDatabase() throws Exception {
        java.sql.Connection dbConnection = null;
        try {
            dbConnection = dataSource.getConnection();
            dbConnection.setAutoCommit(false);
        } catch (Exception ex) {
            log.error("MessageProcessorBase.connectToDatabase(), Exception: " + ex, ex);
            throw ex;
        }

        return dbConnection;
    }

    private void closeConnectionToDatabase(long messageId, java.sql.Connection conn) {
        String logInfo = msgTypeForThisProcessor + ".closeConnectionToDatabase(msgID:" + messageId + ") ";
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            log.error(logInfo + " Exception: ", e, e);
        }
    }

    /**
     * Method to put the message properties hashMap back into the given Message
     *
     * @param msg
     * @param properties
     * @return Message with Properties added
     */
    private Message setMessageProperties(Message msg, HashMap<String, Object> properties) throws JMSException {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String propertyName = entry.getKey();
            Object value = entry.getValue();
            msg.setObjectProperty(propertyName, value);
        }
        return msg;
    }

    /**
     * Method to put the message properties hashMap back into the given Message
     *
     * @param msg
     * @param properties
     * @return Message with Properties added
     */
    private Message setMessageProperties(Message msg, Map<String, Object> properties) throws JMSException {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String propertyName = entry.getKey();
            Object value = entry.getValue();
            msg.setObjectProperty(propertyName, value);
        }
        return msg;
    }


    /**
     * Create a new message properties out of existing.
     */
    protected Map<String, Object> createMessagePropertiesFromExisting(Map<String, Object> map) throws Exception {
        HashMap<String, Object> msgProperties = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            msgProperties.put(entry.getKey(), entry.getValue());
        }
        return msgProperties;
    }

    public void readConfig() {
        try {
            String temp = getConfig("REPLAY_LIMIT");
            if (temp != null) {
                try {
                    REPLAY_LIMIT = Integer.valueOf(temp);
                } catch (Exception ex) {
                    REPLAY_LIMIT = 5;
                }
            }
            temp = getConfig("REPLAY_DELAY_DEFAULT_MS");
            if (temp != null) {
                try {
                    REPLAY_DELAY_DEFAULT_MS = Long.valueOf(temp);
                } catch (Exception ex) {
                    REPLAY_DELAY_DEFAULT_MS = 2000;
                }
            }
            temp = getConfig("REPLAY_DELAY_FIRST_MS");
            if (temp != null) {
                try {
                    REPLAY_DELAY_FIRST_MS = Long.valueOf(temp);
                } catch (Exception ex) {
                    REPLAY_DELAY_FIRST_MS = 2000;
                }
            }
            temp = getConfig("REPLAY_DELAY_SECOND_MS");
            if (temp != null) {
                try {
                    REPLAY_DELAY_SECOND_MS = Long.valueOf(temp);
                } catch (Exception ex) {
                    REPLAY_DELAY_SECOND_MS = 5000;
                }
            }
            temp = getConfig("REPLAY_DELAY_THIRD_MS");
            if (temp != null) {
                try {
                    REPLAY_DELAY_THIRD_MS = Long.valueOf(temp);
                } catch (Exception ex) {
                    REPLAY_DELAY_THIRD_MS = 10000;
                }
            }
            temp = getConfig("REPLAY_DELAY_FORTH_MS");
            if (temp != null) {
                try {
                    REPLAY_DELAY_FORTH_MS = Long.valueOf(temp);
                } catch (Exception ex) {
                    REPLAY_DELAY_FORTH_MS = 20000;
                }
            }
            temp = getConfig("REPLAY_DELAY_FIFTH_MS");
            if (temp != null) {
                try {
                    REPLAY_DELAY_FIFTH_MS = Long.valueOf(temp);
                } catch (Exception ex) {
                    REPLAY_DELAY_FIFTH_MS = 30000;
                }
            }
            temp = getConfig("REPLAY_ERROR_DELAY_MS");
            if (temp != null) {
                try {
                    REPLAY_ERROR_DELAY_MS = Long.valueOf(temp);
                } catch (Exception ex) {
                    REPLAY_ERROR_DELAY_MS = 20000;
                }
            }
            temp = getConfig("SEND_NO_MSG_ID_FOUND_TO_ERROR");
            if (temp != null) {
                try {
                    SEND_NO_MSG_ID_FOUND_TO_ERROR = Boolean.valueOf(temp);
                } catch (Exception ex) {
                    SEND_NO_MSG_ID_FOUND_TO_ERROR = true;
                }
            }
        } catch (Exception ex) {
            log.error("MessageProcessorBase.readConfig(),  Exception: ", ex);
        }

    }

    /**
     * <p>Description       : method to get the value from AppConfigs class </p>
     *
     * @param key String
     * @return null
     * @see com.fedex.intl.cd.configuration.AppConfigs
     * @see com.fedex.intl.cd.utils.SpringContext
     */
    public String getConfig(String key) {
        try {
            if ((appConfigs != null) && (appConfigs.getGeneral() != null)) {
                return appConfigs.getGeneral().get(key);
            } else {
                log.warn("MessageProcessorBase.getConfig(), Config is null or invalid");
            }
        } catch (Exception ex) {
            log.error("MessageProcessorBase.getConfig(), Exception getting reference to configuration. Exception: ", ex);
        }
        return null;
    }

}
