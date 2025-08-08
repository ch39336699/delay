package com.fedex.intl.cd.jms.publish;

import com.fedex.intl.cd.defines.ResourceDefineBase;
import com.fedex.intl.cd.utils.CryptoUtils;
import com.fedex.intl.cd.utils.SpringContext;
import com.fedex.intl.cd.utils.TeamsNotifier;
import com.fedex.mi.decorator.jms.FedexJmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Headers;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
public class PublisherBase {

    @Value("${url.ldap}")
    protected String ldapUrl;

    @Value("${initialContext}")
    protected String fdxTibcoInitialContext;

    @Autowired
    protected TeamsNotifier teamsNotifier;

    @Autowired
    private CryptoUtils cryptoUtil;

    public MessageProducer producer = null;

    public com.fedex.intl.cd.configuration.AppConfigs appConfigs;
    public Session session = null;
    protected InitialContext initialContext = null;
    protected Connection connection = null;
    public String queueName = null;
    public int publishedCnt;

    protected String DASHBOARD_URL;
    protected int MSTEAMS_ALERT_INTERVAL_SECONDS;

    protected void initialize(String name, String factoryNm, String queueNm, String ldapUserName, String encryptedQueuePassword) {
        try {
            queueName = queueNm;
            if (((queueName != null) && (queueName.length() > 25)) && ((factoryNm != null) && (factoryNm.length() > 12))) {
                Destination destination = getDestination(queueName);
                FedexJmsConnectionFactory connectionFactory = createConnectionFactory(name, factoryNm, ldapUserName, encryptedQueuePassword);
                connection = connectionFactory.createConnection();
                session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                producer = session.createProducer(destination);
                connection.start();
                JSONObject jmsStats = new JSONObject();
                jmsStats.put("name", name);
                jmsStats.put("type", "publisher");
                jmsStats.put("queue", queueName);
                jmsStats.put("factory", factoryNm);
                log.info("PublisherBase.initialize(),", kv("MsgFactoryStat", jmsStats));
            } else {
                log.info("PublisherBase.initialize(), No Data set for [ Publisher ] ConnectionFactory queue: {} ldapUrl: {} ldapUserName: {} queueName: {} factoryName: {}", name, ldapUrl, ldapUserName, queueName, factoryNm);
            }
        } catch (Exception ex) {
            closeConnections();
            log.error("PublisherBase.initialize(), Exception:", ex);
            log.error("PublisherBase.initialize(), Error getting LDAP connection to queue:{} factory:{} Exception: {}", queueNm, factoryNm, ex.getMessage());
        }
    }

    protected Destination getDestination(String qName) {
        Destination destination = null;
        try {
            destination = (Destination) getFedexTibcoInitialContext().lookup(qName);
        } catch (NamingException ex) {
            log.error("PublisherBase.getDestination(), NamingException:", ex);
        }
        return destination;
    }

    private InitialContext getFedexTibcoInitialContext() throws NamingException {
        Properties props = new Properties();
        props.put(Context.INITIAL_CONTEXT_FACTORY, fdxTibcoInitialContext);
        props.put(Context.PROVIDER_URL, ldapUrl);
        initialContext = new InitialContext(props);
        return initialContext;
    }

    protected FedexJmsConnectionFactory createConnectionFactory(String name, String factoryName, String ldapUserName, String encryptedQueuePassword) throws BadPaddingException, IllegalBlockSizeException {
        FedexJmsConnectionFactory connectionFactory = null;
        try {
            log.debug("PublisherBase.createConnectionFactory(), [ Publisher ] ConnectionFactory queue: {} ldapUrl: {} ldapUserName: {} queueName: {} factoryName: {}", name, ldapUrl, ldapUserName, queueName, factoryName);
            connectionFactory = (FedexJmsConnectionFactory) getFedexTibcoInitialContext().lookup(factoryName);
            connectionFactory.setUser(ldapUserName);
            String pass = cryptoUtil.getDecryptedString(encryptedQueuePassword);
            connectionFactory.setPassword(pass);
        } catch (NamingException e) {
            log.error("PublisherBase.createConnectionFactory(), NamingException:", e);
        } catch (Exception ex) {
            log.error("PublisherBase.createConnectionFactory(), Exception:", ex);
        }
        return connectionFactory;
    }


    /**
     *
     */
    public void closeConnections() {
        log.trace("PublisherBase.closeConnections(), started.");

        //close Initial Context
        try {
            if (initialContext != null) {
                initialContext.close();
                log.debug("PublisherBase.closeConnections(), Closed initialContext.");
            }
        } catch (Exception ex) {
            log.error("PublisherBase.closeConnections(), " + queueName + " Failed to close initialContext. Exception: ", ex);
        } finally {
            initialContext = null;
        }

        //close producer
        try {
            if (producer != null) {
                producer.close();
                if (producer != null) {
                    log.debug("PublisherBase.closeConnections(), Closed producer: {}", producer.getClass().getName());
                }
            }
        } catch (Exception ex) {
            log.error("PublisherBase.closeConnections(), Failed to close producer. Exception: ", ex);
        } finally {
            producer = null;
        }

        //close session
        try {
            if (session != null) {
                session.close();
                if (session != null) {
                    log.debug("PublisherBase.closeConnections(), Closed session: {}", session.getClass().getName());
                }
            }
        } catch (Exception ex) {
            log.error("PublisherBase.closeConnections(), Failed to close session. Exception: ", ex);
        } finally {
            session = null;
        }

        //close connection
        try {
            if (connection != null) {
                connection.close();
                if (connection != null) {
                    log.debug("PublisherBase.closeConnections(), Closed connection: {}", connection.getClass().getName());
                }
            }
        } catch (Exception ex) {
            log.error("PublisherBase.closeConnections(), Failed to close connection. Exception: ", ex);
        } finally {
            connection = null;
        }
        //Done
        log.trace("PublisherBase.closeConnections(), Done.");
    }


    /**
     * create the byte message with properties passed in.
     * if the send failed, attempt to close any open connections, restart connections, and resend
     * this is in case temporary loss of connection to MQ
     * if the retry failed, throw exception for main process to handle error situations
     */
   /* public void sendIt(String name, String factory, byte[] messagePayLoad, Map <String, Object> messageProperties, long messageID) throws Exception {
        BytesMessage bmsg = session.createBytesMessage();
        bmsg.clearBody();
        bmsg.writeBytes(messagePayLoad);
        if (messageProperties != null) {
            for (Map.Entry <String, Object> entry : messageProperties.entrySet()) {
                //log.info("Key = " + entry.getKey() +", Value = " + entry.getValue());
                String value = entry.getValue().toString();
                bmsg.setObjectProperty(entry.getKey(), value);
            }
        }
        producer.send(bmsg);
        publishedCnt++;
        log.debug("PublisherBase.sendIt(byte[]) [queue: " + name + "] [factory: " + factory + "] [msgID: " + messageID + "] published byte[]");
    }*/
    protected void sendIt(String name, String factory, byte[] messagePayLoad) throws JMSException {
        BytesMessage message = session.createBytesMessage();
        message.clearBody();
        message.writeBytes(messagePayLoad);
        producer.send(message);
        if (log.isDebugEnabled()) {
            log.debug("PublisherBase.sendIt(byte[]) [queue: " + name + "] [factory: " + factory + "] published byte[]");
        }
    }

    protected void sendIt(String name, String factory, BytesMessage message) throws JMSException {
        producer.send(message);
        if (log.isDebugEnabled()) {
            log.debug("PublisherBase.sendIt(BytesMessage), [queue: " + name + "] [factory: " + factory + "] published BytesMessage.");
        }
    }

    protected void sendIt(String name, String factory, Message message) throws JMSException {
        producer.send(message);
        if (log.isDebugEnabled()) {
            log.debug("PublisherBase.sendIt(BytesMessage), [queue: " + name + "] [factory: " + factory + "] published BytesMessage.");
        }
    }

    /**
     *
     */
    public BytesMessage createByteMessage(Map<String, Object> messageProperties, byte messageData[]) {
        try {
            BytesMessage bmsg = session.createBytesMessage();
            bmsg.clearBody();
            bmsg.writeBytes(messageData);
            if (messageProperties != null) {
                for (Map.Entry<String, Object> entry : messageProperties.entrySet()) {
                    //log.info("Key = " + entry.getKey() +", Value = " + entry.getValue());
                    String value = entry.getValue().toString();
                    bmsg.setObjectProperty(entry.getKey(), value);
                }
            }
            return bmsg;
        } catch (Exception ex) {
            if (session == null) {
                log.warn("PublisherBase.createByteMessage(), Session is null.");
            }
            log.error("PublisherBase.createByteMessage(), Exception: ", ex);
        }
        return null;
    }

    public BytesMessage createByteMessage(String messageProps[][], byte messageData[]) {
        try {
            BytesMessage bmsg = session.createBytesMessage();
            bmsg.clearBody();
            if (messageData != null) {
                bmsg.writeBytes(messageData);
            } else {
                log.warn("PublisherBase.createByteMessage(), messageData is null. ");
                throw new RuntimeException("MessageData is null when setting to outbound message.");
            }
            if (messageProps != null) {
                setStringPropForMsg(bmsg, messageProps);
            }
            return bmsg;
        } catch (Exception ex) {
            if (session == null) {
                log.warn("PublisherBase.createByteMessage(), Session is null.");
            }
            log.error("PublisherBase.createByteMessage(), Exception:", ex);
        }
        return null;
    }

    protected void setStringPropForMsg(BytesMessage msg, String props[][]) throws Exception {
        for (int i = 0; i < props.length; i++) {
            msg.setStringProperty(props[i][0], props[i][1]);
        }
        //Add or override MsgSource to CORP
        // msg.setStringProperty("MsgSource", CorpDefinesBase.AGGREGATE_STATUS_SOURCE);
    }

    protected void sendMSTeamAlert(String title, String message) {
        try {
            if (MSTEAMS_ALERT_INTERVAL_SECONDS == 0) {
                readConfigBase();
            }
            long alertTimeDifference = teamsNotifier.getTimeSinceLastAlert();
            if ((alertTimeDifference > 0) && (alertTimeDifference < MSTEAMS_ALERT_INTERVAL_SECONDS)) {
                //Hasn't been long enough since the last alert was sent.
                if (log.isDebugEnabled()) {
                    log.debug("PublisherBase.sendMSTeamAlert(), MS Teams message not set. Hasn't been long enough since the last alert was sent. alertTimeDifference: {} < MSTEAMS_ALERT_INTERVAL_SECONDS: {}", alertTimeDifference, MSTEAMS_ALERT_INTERVAL_SECONDS);
                }
            } else {
                teamsNotifier.sendNotification(TeamsNotifier.ERROR, title, "Contact CDV on call", message, DASHBOARD_URL);
            }
        } catch (Exception ex) {
            log.error("PublisherBase.sendMSTeamAlert), Exception:", ex);
        }
    }

    public String getMsgProps(Map msgProps) {
        StringBuffer props = new StringBuffer("[");
        try {
            Iterator it = msgProps.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                props.append("{");
                props.append(pair.getKey());
                props.append(",");
                props.append(pair.getValue());
                props.append("}");
            }
            props.append("]");
            return props.toString();
        } catch (Exception ex) {
            log.error("PublisherBase.getMsgProps(), Exception:", ex);
        }
        return "";
    }

    public void readConfigBase() {
        try {
            String temp = getConfig("MSTEAMS_ALERT_INTERVAL_SECONDS");
            if (temp != null) {
                try {
                    MSTEAMS_ALERT_INTERVAL_SECONDS = Integer.valueOf(temp);
                } catch (Exception ex) {
                    //Default to 10 min
                    MSTEAMS_ALERT_INTERVAL_SECONDS = 600;
                }
            } else {
                //Default to 10 min
                MSTEAMS_ALERT_INTERVAL_SECONDS = 600;
            }
        } catch (Exception ex) {
            log.warn("PublisherBase.readConfigBase(), MSTEAMS_ALERT_INTERVAL_SECONDS not in config. Setting to default 600 seconds (10 min).");
            //Default to 10 min
            MSTEAMS_ALERT_INTERVAL_SECONDS = 600;
        }

        try {
            String temp = getConfig("DASHBOARD_URL");
            if (temp != null) {
                try {
                    DASHBOARD_URL = temp;
                } catch (Exception ex) {
                    DASHBOARD_URL = null;
                }
            }
        } catch (Exception ex) {
            log.warn("PublisherBase.readConfigBase(), DASHBOARD_URL not in config. Setting to default null.");
            DASHBOARD_URL = null;
        }
    }

    /**
     <p>Description       : method to get the value from AppConfigs class </p>
     @param key String
     @return null
     @see com.fedex.intl.cd.configuration.AppConfigs
     */
    public String getConfig(String key) {
        try {
            if (appConfigs == null) {
                appConfigs = SpringContext.getBean(com.fedex.intl.cd.configuration.AppConfigs.class);
            }
            if ((appConfigs != null) && (appConfigs.getGeneral() != null)) {
                return appConfigs.getGeneral().get(key);
            } else {
                log.warn("PublisherBase.getConfig(), Config is null or invalid");
            }
        } catch (Exception ex) {
            log.error("PublisherBase.getConfig(), Exception getting reference to configuration. Exception:", ex);
        }
        return null;
    }

    protected boolean isHealthPing(String msg) {
        return msg.startsWith("HEALTH");
    }

    protected boolean isNonHealthPing(String msg) {
        return !isHealthPing(msg);
    }

    protected void addMessageProperty() {

    }
}
