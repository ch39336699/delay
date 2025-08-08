package com.fedex.intl.cd.jms.publish;

import com.fedex.intl.cd.configuration.AppConfigs;
import com.fedex.intl.cd.defines.ResourceDefineBase;
import com.fedex.intl.cd.utils.TeamsNotifier;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Service
public class ErrorJMSPublisher extends PublisherBase {

    private final String name = "errorQueue";

    int ERROR_QUEUE_MSG_COUNT_INFO;
    int ERROR_QUEUE_MSG_COUNT_WARN;
    int ERROR_QUEUE_MSG_COUNT_SEVERITY;
    int ERROR_QUEUE_MSG_ALERT_TIMEFRAME_SECONDS;
    String DASHBOARD_URL;

    private String errorQueueLastSetTime;
    private String firstErrorQueueLastSetTime;
    private int errorQueueCount;

    @Value("${" + name + ".factory:null}")
    public String factoryName;

    @Value("${" + name + ".name:null}")
    public String queueName;

    @Value("${" + name + ".userName:null}")
    protected String ldapUserName;

    @Value("${" + name + ".encryptedPW:null}")
    protected String encryptedQueuePassword;

    @Autowired
    protected TeamsNotifier teamsNotifier;

    @Autowired
    public AppConfigs appConfigs;

    @PostConstruct
    public void setup() throws Exception {
        try {
            if (log.isTraceEnabled()) {
                log.trace("ErrorJMSPublisher.setup(), Called.");
            }
            this.initialize(name, factoryName, "fxClientDestinationUID=D." + queueName, ldapUserName, encryptedQueuePassword);
        } catch (Exception ex) {
            log.error("ErrorJMSPublisher.setup(), Exception:", ex);
        }
    }

    @PreDestroy
    public void destroy() {
        if (log.isTraceEnabled()) {
            log.trace("ErrorJMSPublisher.destroy(), Called.");
        }
        finalize();
    }

    public boolean send(byte[] messagePayLoad, String trackingNbr, String trackType) throws BadPaddingException, IllegalBlockSizeException, JMSException {
        boolean sent = false;
        try {
            if ((producer == null) || (session == null)) {
                log.warn("ErrorJMSPublisher.send(byte[]), Producer or Session is null for {} ...initializing", name);
                this.initialize(name, factoryName, "fxClientDestinationUID=D." + queueName, ldapUserName, encryptedQueuePassword);
            } else {
                try {
                    //Will throw an exception if producer is closed
                    producer.getTimeToLive();
                } catch (Exception ex) {
                    log.info("ErrorJMSPublisher.send(byte[]), Producer is closed for {} ...initializing", name);
                    this.initialize(name, factoryName, "fxClientDestinationUID=D." + queueName, ldapUserName, encryptedQueuePassword);
                }
            }
            if ((producer != null) && (session != null)) {
                super.sendIt(name, factoryName, messagePayLoad);
                sent = true;
                checkErrorMsgsPublished();
            } else {
                log.warn("ErrorJMSPublisher.send(byte[]), Producer or Session is null. Message not published to queue: {} factory: {}", queueName, factoryName);
                sendMSTeamAlert("Error Publishing to Error Queue", "Producer or session null for " + queueName);
            }
        } catch (Exception ex) {
            String message = null;
            if (!ex.getMessage().contains("Queue limit exceeded")) {
                log.error("ErrorJMSPublisher.send(byte[]), Exception:", ex);
                message = "Exception sending to " + queueName;
            } else {
                log.error("ErrorJMSPublisher.send(byte[]),  Queue limit exceeded sending to error");
                message = "Queue limit exceeded sending to " + queueName;
            }
            sendMSTeamAlert("Error Publishing to Error Queue", message);
        } finally {
            if (sent) {
                JSONObject main = new JSONObject();
                main.put("queue", queueName);
                if ((trackingNbr != null) && trackingNbr.trim().length() > 0) {
                    main.put("trackingNbr", trackingNbr);
                }
                if ((trackType != null) && trackType.trim().length() > 0) {
                    main.put("trackType", trackType);
                }
                log.info("ErrorJMSPublisher.send(byte[]), Published. factory: {}", factoryName, kv("data", main));
            }
        }
        return sent;
    }

    public void send(byte[] messagePayLoad, Map<String, Object> messageProperties, long messageID) throws JMSException {
        boolean sent = false;
        String rulesfile = messageProperties.containsKey(ResourceDefineBase.RULES_FILE) ? (String) messageProperties.get(ResourceDefineBase.RULES_FILE) : "";
        String stateType = messageProperties.containsKey(ResourceDefineBase.STATE_TYPE) ? (String) messageProperties.get(ResourceDefineBase.STATE_TYPE) : "";
        if ((producer == null) || (session == null)) {
            log.warn("ErrorJMSPublisher.send(byte[]), {} Producer or Session is null  for messageID: {} ...initializing", messageID, name);
            this.initialize(name, factoryName, "fxClientDestinationUID=D." + queueName, ldapUserName, encryptedQueuePassword);
        } else {
            try {
                //Will throw an exception if producer is closed
                producer.getTimeToLive();
            } catch (Exception ex) {
                log.info("ErrorJMSPublisher.send(), {} Producer is closed for messageID: {} ...initializing", name, messageID);
                this.initialize(name, factoryName, "fxClientDestinationUID=D." + queueName, ldapUserName, encryptedQueuePassword);
            }
        }

        try {
            if ((producer != null) && (session != null)) {
                BytesMessage bmsg = createByteMessage(messageProperties, messagePayLoad);
                super.sendIt(name, factoryName, bmsg);
                sent = true;
            } else {
                log.warn("ErrorJMSPublisher.send(), Message not published to queue: {} factory: {} messageID: {}", queueName, factoryName, messageID);
            }
        } catch (Exception ex) {
            String message = null;
            if (!ex.getMessage().contains("Queue limit exceeded")) {
                log.error("ErrorJMSPublisher.send(), {} Exception:", ex);
                message = "Exception sending to " + queueName;
            } else {
                log.error("ErrorJMSPublisher.send(), {} Queue limit exceeded sending to XLT. messageID: {}", messageID);
                message = "Queue limit exceeded sending to " + queueName;
            }
            sendMSTeamAlert("Error Publishing to XLTNormal", message);
        } finally {
            if (sent) {
                JSONObject main = new JSONObject();
                main.put("queue", queueName);
                main.put("messageID", messageID);
                main.put("rulesfile", rulesfile);
                main.put("stateType", stateType);
                String header = getMsgProps(messageProperties);
                main.put("header", header);
                String body = new String(messagePayLoad);
                main.put("body", body);
                log.info("ErrorJMSPublisher.send(), Published. messageID: {} factory: {}", messageID, factoryName, kv("data", main));
            }
        }
    }

    public boolean checkErrorMsgsPublished() {
        try {
            String message = null;
            readConfig();
            long alertTimeDifference = teamsNotifier.getTimeSinceLastAlert();
            incrementErrorQueueCount();
            if ((alertTimeDifference > 0) && (alertTimeDifference < MSTEAMS_ALERT_INTERVAL_SECONDS)) {
                //Hasn't been long enough since the last MS Teams alert was sent.
                return false;
            }
            String current = teamsNotifier.getCurrentTime();
            long totalTimeErrorsSent = teamsNotifier.getTimeDurationInMilliSec(getFirstErrorQueueLastSetTime(), current);
            if (totalTimeErrorsSent < 60000) {
                //Less than a minute
                long seconds = TimeUnit.MILLISECONDS.toSeconds(totalTimeErrorsSent);
                message = getErrorQueueCount() + " messages sent under 1 minute to error queue: " + queueName;
            } else {
                long minutes = TimeUnit.MILLISECONDS.toMinutes(totalTimeErrorsSent);
                message = getErrorQueueCount() + " messages sent within " + minutes + " minute(s) to error queue: " + queueName;
            }
            if ((ERROR_QUEUE_MSG_COUNT_SEVERITY > 0) && (getErrorQueueCount() > ERROR_QUEUE_MSG_COUNT_SEVERITY)) {
                teamsNotifier.sendNotification(TeamsNotifier.ERROR, "Error Message Count Critical", "Contact CDV on call", message, DASHBOARD_URL);
                if (log.isDebugEnabled()) {
                    log.debug("ErrorJMSPublisher.checkErrorMsgsPublished(), {}", message);
                }
                return true;
            } else if ((ERROR_QUEUE_MSG_COUNT_WARN > 0) && (getErrorQueueCount() > ERROR_QUEUE_MSG_COUNT_WARN)) {
                teamsNotifier.sendNotification(TeamsNotifier.WARNING, "Error Message Count High", "Check EIT Error queue", message, DASHBOARD_URL);
                if (log.isDebugEnabled()) {
                    log.debug("ErrorJMSPublisher.checkErrorMsgsPublished(), {}", message);
                }
                return true;
            } else if ((ERROR_QUEUE_MSG_COUNT_INFO > 0) && (getErrorQueueCount() > ERROR_QUEUE_MSG_COUNT_INFO)) {
                teamsNotifier.sendNotification(TeamsNotifier.INFO, "Error Message Count Growing", "Continue to monitor", message, DASHBOARD_URL);
                if (log.isDebugEnabled()) {
                    log.debug("ErrorJMSPublisher.checkErrorMsgsPublished(), {}", message);
                }
                return true;
            }
        } catch (Exception ex) {
            log.error("ErrorJMSPublisher.checkErrorMsgsPublished(), Exception:", ex);
        }
        return false;
    }

    public void readConfig() {

        try {
            String temp = appConfigs.getGeneral().get("ERROR_QUEUE_MSG_COUNT_INFO");
            if (temp != null) {
                try {
                    ERROR_QUEUE_MSG_COUNT_INFO = Integer.valueOf(temp);
                } catch (Exception ex) {
                    ERROR_QUEUE_MSG_COUNT_INFO = 0;
                }
            }
        } catch (Exception ex) {
            log.error("ErrorJMSPublisher.readConfig(), The property ERROR_QUEUE_MSG_COUNT_INFO was not set in the .yml file. Exception: ", ex);
            ERROR_QUEUE_MSG_COUNT_INFO = 0;
        }

        try {
            String temp = appConfigs.getGeneral().get("ERROR_QUEUE_MSG_COUNT_WARN");
            if (temp != null) {
                try {
                    ERROR_QUEUE_MSG_COUNT_WARN = Integer.valueOf(temp);
                } catch (Exception ex) {
                    ERROR_QUEUE_MSG_COUNT_WARN = 0;
                }
            }
        } catch (Exception ex) {
            log.error("ErrorJMSPublisher.readConfig(), The property ERROR_QUEUE_MSG_COUNT_WARN was not set in the .yml file. Exception: ", ex);
            ERROR_QUEUE_MSG_COUNT_WARN = 0;
        }

        try {
            String temp = appConfigs.getGeneral().get("ERROR_QUEUE_MSG_COUNT_SEVERITY");
            if (temp != null) {
                try {
                    ERROR_QUEUE_MSG_COUNT_SEVERITY = Integer.valueOf(temp);
                } catch (Exception ex) {
                    ERROR_QUEUE_MSG_COUNT_SEVERITY = 0;
                }
            }
        } catch (Exception ex) {
            log.error("ErrorJMSPublisher.readConfig(), The property ERROR_QUEUE_MSG_COUNT_SEVERITY was not set in the .yml file. Exception: ", ex);
            ERROR_QUEUE_MSG_COUNT_SEVERITY = 0;
        }

        try {
            String temp = appConfigs.getGeneral().get("ERROR_QUEUE_MSG_ALERT_TIMEFRAME_SECONDS");
            if (temp != null) {
                try {
                    ERROR_QUEUE_MSG_ALERT_TIMEFRAME_SECONDS = Integer.valueOf(temp);
                } catch (Exception ex) {
                    ERROR_QUEUE_MSG_ALERT_TIMEFRAME_SECONDS = 0;
                }
            }
        } catch (Exception ex) {
            log.error("ErrorJMSPublisher.readConfig(), The property ERROR_QUEUE_MSG_ALERT_TIMEFRAME_SECONDS was not set in the .yml file. Exception: ", ex);
            ERROR_QUEUE_MSG_ALERT_TIMEFRAME_SECONDS = 0;
        }

        try {
            String temp = appConfigs.getGeneral().get("MSTEAMS_ALERT_INTERVAL_SECONDS");
            if (temp != null) {
                try {
                    MSTEAMS_ALERT_INTERVAL_SECONDS = Integer.valueOf(temp);
                } catch (Exception ex) {
                    MSTEAMS_ALERT_INTERVAL_SECONDS = 0;
                }
            }
        } catch (Exception ex) {
            log.error("ErrorJMSPublisher.readConfig(), The property MSTEAMS_ALERT_INTERVAL_SECONDS was not set in the .yml file. Exception: ", ex);
            MSTEAMS_ALERT_INTERVAL_SECONDS = 0;
        }

        try {
            String temp = appConfigs.getGeneral().get("DASHBOARD_URL");
            if (temp != null) {
                try {
                    DASHBOARD_URL = temp;
                } catch (Exception ex) {
                    DASHBOARD_URL = null;
                }
            }
        } catch (Exception ex) {
            log.error("ErrorJMSPublisher.readConfig(), The property DASHBOARD_URL was not set in the .yml file. Exception: ", ex);
            DASHBOARD_URL = null;
        }
    }


    public void setFirstErrorQueueLastSetTime() {
        firstErrorQueueLastSetTime = teamsNotifier.getCurrentTime();
    }

    public String getFirstErrorQueueLastSetTime() {
        if (firstErrorQueueLastSetTime == null) {
            firstErrorQueueLastSetTime = teamsNotifier.getCurrentTime();
        }
        return firstErrorQueueLastSetTime;
    }

    public void setErrorQueueLastSetTime() {
        errorQueueLastSetTime = teamsNotifier.getCurrentTime();
    }

    public String getErrorQueueLastSetTime() {
        if (errorQueueLastSetTime == null) {
            errorQueueLastSetTime = teamsNotifier.getCurrentTime();
        }
        return errorQueueLastSetTime;
    }

    public int getErrorQueueCount() {
        return errorQueueCount;
    }

    public void incrementErrorQueueCount() {
        long timeDifference = teamsNotifier.getTimeSinceLastAlert();
        if ((timeDifference > 0) && (timeDifference < ERROR_QUEUE_MSG_ALERT_TIMEFRAME_SECONDS)) {
            errorQueueCount++;
        } else {
            errorQueueCount = 1;
            setFirstErrorQueueLastSetTime();
        }
        setErrorQueueLastSetTime();
    }

    protected void finalize() {
        try {
            if (log.isDebugEnabled()) {
                log.debug("ErrorJMSPublisher.finalize(), Closing EIT connections.");
            }
            closeConnections();
        } catch (Exception ex) {
            log.error("ErrorJMSPublisher.finalize(), Exception:", ex);
        }
    }

}
