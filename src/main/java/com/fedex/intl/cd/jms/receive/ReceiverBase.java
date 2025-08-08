package com.fedex.intl.cd.jms.receive;

import com.fedex.intl.cd.configuration.AppConfigs;
import com.fedex.intl.cd.defines.ResourceDefineBase;
import com.fedex.intl.cd.errorHandler.JmsEitErrorHandler;
import com.fedex.intl.cd.model.DelayedMessage;
import com.fedex.intl.cd.scheduled.Statistics;
import com.fedex.intl.cd.utils.CryptoUtils;
import com.fedex.intl.cd.utils.DelayedMessageStats;
import com.fedex.intl.cd.worker.CorpShipmentService;
import com.fedex.intl.cd.worker.MDERetryService;
import com.fedex.intl.cd.worker.XLTNormalService;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.json.simple.JSONObject;
import org.springframework.aop.target.CommonsPool2TargetSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.connection.UserCredentialsConnectionFactoryAdapter;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jndi.JndiTemplate;
import org.springframework.messaging.handler.annotation.Headers;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.NamingException;
import java.util.*;

import static net.logstash.logback.argument.StructuredArguments.kv;


@Slf4j
public abstract class ReceiverBase {

    private final String name = "target";


    public static List<String> lastUUIDs = new ArrayList<>();

    //public static String currentUUID;

    @Autowired
    private CryptoUtils cryptoUtil;

    @Autowired()
    public AppConfigs appConfigs;

    @Autowired
    @Qualifier("TargetPool")
    CommonsPool2TargetSource pooledTargetSource;

    @Value("${initialContext}")
    protected String fdxTibcoInitialContext;

    @Autowired
    MDERetryService mdeRetryService;
    @Autowired
    CorpShipmentService corpShipmentService;

    @Autowired()
    private DelayedMessageStats delayedMessageStats;

    @Autowired
    public Statistics statistics;

    @Value("${url.ldap}")
    protected String ldapUrl;

    @Value("${encryption.seed}")
    protected String seed;

    @Value("${" + name + ".pool.maxSize:40}")
    protected int rulesProcMaxSize;

    @Value("${" + name + ".pool.recycle:1000}")
    protected int recycle;

    @Value("${" + name + ".pool.runTime:5000}")
    protected int runTime;

    protected boolean PRINT_MSGS_RECEIVED = true;

    public List<XLTNormalService> _available = Collections.synchronizedList(new ArrayList<XLTNormalService>());

    public JmsListenerContainerFactory<DefaultMessageListenerContainer> createMessageFactory(String name, String queueName, String factoryName, String concurrency, long recoveryInterval, String ldapUserName, String encryptedQueuePassword) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        try {
            // Decrypt the LDAP Password
            String pass = cryptoUtil.getDecryptedString(encryptedQueuePassword);
            factory.setConnectionFactory(
                    getConnectionFactory(name, ldapUrl, ldapUserName, pass, queueName, factoryName));
        } catch (Exception e) {
            log.error("ReceiverBase.createMessageFactory(), Exception:", e);
        }
        factory.setConcurrency(concurrency);
        factory.setRecoveryInterval(recoveryInterval);
        factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
        factory.setSessionTransacted(true);
        // factory.setCacheLevel(DefaultMessageListenerContainer.CACHE_CONSUMER);
        factory.setErrorHandler(new JmsEitErrorHandler());
        return factory;
    }

    private ConnectionFactory getConnectionFactory(String name, String ldapUrl, String ldapUserName,
                                                   String password, String queueName, String factoryName) throws NamingException, Exception {
        JndiTemplate jndiTemplate = new JndiTemplate();
        UserCredentialsConnectionFactoryAdapter ucf = null;
        Properties props = new Properties();
        log.info("==== [ Receiver ] ConnectionFactory queue: {} ldapUrl: {} ldapUserName: {} queueName: {} factoryName: {} ====", name, ldapUrl, ldapUserName, queueName, factoryName);

        props.put(Context.INITIAL_CONTEXT_FACTORY, fdxTibcoInitialContext);
        props.put(Context.PROVIDER_URL, ldapUrl);
        props.put(Context.SECURITY_PRINCIPAL, ldapUserName);
        props.put(Context.SECURITY_CREDENTIALS, password);
        jndiTemplate.setEnvironment(props);
        ConnectionFactory connectionFactory = (ConnectionFactory) jndiTemplate.lookup(factoryName);
        ucf = new UserCredentialsConnectionFactoryAdapter();
        ucf.setUsername(ldapUserName);
        ucf.setPassword(password);
        ucf.setTargetConnectionFactory(connectionFactory);
        return ucf;
    }

    protected void internalProcess(Message msg) {
        XLTNormalService xltNormalService = null;
        DelayedMessage message = null;
        HashMap<String, Object> messageProperties = new HashMap<String, Object>();
        boolean successful = false;
        try {
            try {
                messageProperties = getMessageProperties((ActiveMQBytesMessage) msg);
            } catch (Exception ex) {
                log.error("ReceiverBase.internalProcess(), Exception. Keep going.", ex);
            }
            if (messageProperties.containsKey(ResourceDefineBase.EXTERNAL_PUB)) {
                message = delayedMessageStats.dequeueRetryMessage((ActiveMQBytesMessage) msg, (String) messageProperties.get(ResourceDefineBase.EXTERNAL_PUB));
                String key = ((String) messageProperties.get(ResourceDefineBase.EXTERNAL_PUB)).trim();
                if ((key != null) && (key.length() > 0)) {
                    switch (key) {
                        case "MDE_RETRY":
                            if (mdeRetryService.onMessageInternal(msg, messageProperties, message)) {
                                successful = true;
                            } else {
                                log.warn("ReceiverBase.internalProcess(), Error sending MDE_RETRY message.");
                            }
                            break;
                        case "CORP_SHIP":
                            if (corpShipmentService.onMessageInternal(msg, messageProperties, message)) {
                                successful = true;
                            } else {
                                log.warn("ReceiverBase.internalProcess(), Error sending CORP_SHIP message.");
                            }
                            break;
                        default:
                            // code block
                    }
                } else {
                    log.warn("ReceiverBase.internalProcess(), No value set in properties for externalPub");
                }
            } else {
                synchronized (this._available) {
                    // if (hasDuplicate((String) messageProperties.get("UUID"))) {
                    //     return;
                    // } else {
                    message = delayedMessageStats.dequeueMessage((ActiveMQBytesMessage) msg);
                    xltNormalService = this.acquireReusable();
                    //if (xltNormalService.onMessageInternal(msg, messageProperties, message)) {
                    //    successful = true;
                    // } else {
                    //     log.warn("ReceiverBase.internalProcess(), Error sending XLTNormal message.");
                    // }
                    // if (xltNormalService != null) {
                    //          this.releaseReusable(xltNormalService);
                    //  }
                    //}
                }
                if (xltNormalService.onMessageInternal(msg, messageProperties, message)) {
                    successful = true;
                } else {
                    log.warn("ReceiverBase.internalProcess(), Error sending XLTNormal message.");
                }
                if (xltNormalService != null) {
                    synchronized (this._available) {
                        this.releaseReusable(xltNormalService);
                    }
                }
            }
        } catch (Exception ex) {
            // This is a fail-safe exception if somehow there is a crack in the EIT replay/DLQ logic and throws an
            // exception. This will put the message in the activeMQ.DLQ after a number of retries putting it back to the
            // original activeMQ queue.
            log.error("ReceiverBase.internalProcess(), Exception, doing session recover: " + ex, ex);
            // use recover if non-transacted, rollback for transacted.
            // session.recover();
        } finally {
            if (successful) {
                statistics.xltNormalMsgCount--;
            }
        }
    }

    /**
     * Get the Message Properties
     */
    protected HashMap<String, Object> getMessageProperties(Message inputMessage) throws Exception {
        HashMap<String, Object> msgProperties = new HashMap<String, Object>();

        Enumeration<String> list = inputMessage.getPropertyNames();
        while (list.hasMoreElements()) {
            String key = list.nextElement();
            msgProperties.put(key, inputMessage.getObjectProperty(key));
            if (log.isTraceEnabled()) {
                log.trace("ReceiverBase.getMessageProperties(), msg properties: {} = {}", key, inputMessage.getStringProperty(key));
            }
        }
        return msgProperties;
    }

    public boolean hasDuplicate(String uuid) {
        synchronized (lastUUIDs) {
            if (lastUUIDs.contains(uuid)) {
                return true;
            } else {
                addUUID(uuid);
                return false;
            }
        }
    }
    public void addUUID(String uuid)
    {
        synchronized (lastUUIDs) {
            if (lastUUIDs.size() > 200) {
                lastUUIDs.remove(0);
            }
            lastUUIDs.add(uuid);
        }
    }

    /**
     * Makes sure our message is a valid message.
     */
    protected byte[] getMessagePayload(Message inputMessage, String queueName) throws Exception {
        byte[] msgPayload = null;

        if (inputMessage == null) {
            log.error("ReceiverBase.getMessagePayload(), queue: {} received null message.", queueName);
            throw new Exception("ReceiverBase.getMessagePayload(), Queue received null message.");
        }

        try {
            if (inputMessage instanceof BytesMessage) {
                BytesMessage byteMsg = (BytesMessage) inputMessage;
                if (byteMsg.getBodyLength() == 0) {
                    log.error("ReceiverBase.getMessagePayload(), queue: {} received a byte message of length 0.", queueName);
                    throw new Exception("byteMsg length is 0. ");
                }
                msgPayload = new byte[(int) byteMsg.getBodyLength()];
                byteMsg.readBytes(msgPayload);
            } else if (inputMessage instanceof TextMessage) {
                String textMsg = ((TextMessage) inputMessage).getText();
                if (textMsg == null || textMsg.equals("")) {
                    log.error("ReceiverBase.getMessagePayload(), queue: {} received a null Text Message.", queueName);
                    throw new Exception("TextMessage is null or blank.");
                }
                msgPayload = textMsg.getBytes();
            } else {
                String jmsType = "";
                try {
                    jmsType = inputMessage.getJMSType();
                } catch (Exception ex) {
                    jmsType = "unknown";
                }
                log.error("ReceiverBase.getMessagePayload(), queue: {} received Invalid JmsType: {}", queueName, jmsType);
                //throw new Exception("Invalid JmsType: " + jmsType);
            }
        } catch (Exception ex) {
            throw ex;
        }
        return msgPayload;
    }

    public XLTNormalService acquireReusable() throws Exception {
        try {
            if (_available.size() != 0) {
                for (int count = 0; count < _available.size(); count++) {
                    if (!_available.get(count).inUse) {
                        _available.get(count).inUse = true;
                        _available.get(count).runCount++;
                        return _available.get(count);
                    }
                }

                if (_available.size() < rulesProcMaxSize) {
                    XLTNormalService item = (XLTNormalService) pooledTargetSource.getTarget();
                    item.inUse = true;
                    item.runCount++;
                    _available.add(item);
                    return item;
                } else {
                    log.warn("ReceiverBase.releaseReusable(), Max Pool size reached {} ", rulesProcMaxSize);
                    throw new Exception("XLTNormalService max pool size reached");
                }

            } else if (_available.size() < rulesProcMaxSize) {
                XLTNormalService item = (XLTNormalService) pooledTargetSource.getTarget();
                item.inUse = true;
                item.runCount++;
                _available.add(item);
                return item;
            }
        } catch (Exception ex) {
            log.error("ReceiverBase.acquireReusable(), Exception: ", ex);
        }
        return null;
    }

    public void releaseReusable(XLTNormalService item) {
        try {
            if (item.runCount > this.recycle) {
                //Recycle if run count and reached a certain number
                log.debug(" Recycling item. runCount: {} > recycle: {}", item.runCount, recycle, kv("reason", "runCount"));
                item.runCount = 0;
                _available.remove(item);
                pooledTargetSource.releaseTarget(item);
            } else if (item.lastRunTime > this.runTime) {
                //Recycle if last run time was longer than 5 seconds
                item.runCount = 0;
                _available.remove(item);
                pooledTargetSource.releaseTarget(item);
                log.debug("ReceiverBase.releaseReusable(), Recycling item. lastRunTime: {} > runTime: {}", item.lastRunTime, this.runTime, kv("reason", "runTime"));
            } else {
                item.inUse = false;
            }
        } catch (Exception ex) {
            log.error("ReceiverBase.releaseReusable(), Exception: ", ex);
        }
    }

    protected void printMsg(byte[] messagePayLoad, @Headers Map<String, Object> headers, String uuid, String trackItemNumber) {
        JSONObject eventLogObj = new JSONObject();
        try {
            eventLogObj.put("uuid", uuid);
            eventLogObj.put("trackItemNumber", trackItemNumber);
            if (headers.containsKey(ResourceDefineBase.EXTERNAL_PUB)) {
                eventLogObj.put("XLTMessage", false);
            } else {
                eventLogObj.put("XLTMessage", true);
            }
            String header = getMsgProps(headers);
            String body = new String(messagePayLoad);
            log.info("HEADER: {} BODY: {} ", header, body, kv("Message", eventLogObj));
        } catch (Exception ex) {
            log.error("ReceiverBase.printMsg(), Exception:", ex);
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
            log.error("ReceiverBase.getMsgProps(), Exception:", ex);
        }
        return "";
    }

    public void readConfig() {
        try {
            String temp = getConfig("PRINT_MSGS_RECEIVED");
            if (temp != null) {
                try {
                    PRINT_MSGS_RECEIVED = Boolean.valueOf(temp);
                } catch (Exception ex) {
                    PRINT_MSGS_RECEIVED = true;
                }
            }

        } catch (Exception ex) {
            log.error("ReceiverBase.readConfig(),  Exception: ", ex);
        }

    }

    /**
     * <p>Description: method to get the value from AppConfigs class </p>
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
                log.warn("ReceiverBase.getConfig(), Config is null or invalid");
            }
        } catch (Exception ex) {
            log.error("ReceiverBase.getConfig(), Exception getting reference to configuration. Exception: ", ex);
        }
        return null;
    }

}
