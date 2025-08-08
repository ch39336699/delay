package com.fedex.intl.cd;
/* for putting on messages onto the Active MQ queue*/

import com.fedex.intl.cd.data.MsgId;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jms.JmsProperties;

import javax.jms.*;
import javax.sql.DataSource;
import java.rmi.RemoteException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class testPublishishToActiveMQ {

    @Autowired
    @Qualifier("oracleDataSource")
    private DataSource dataSource;
    protected String insertSQL = "insert into ERASE_PCF_SCHEMA.MSG_ID WHERE MSG_ID = ?";
    public static final String MESSAGE_ID_PROPERTY = "MSG_ID";

    public String topicNm = "erase.topic.xlt";
//   public String topicNm = "erase.topic.relationshipEvnt";
//   public String topicNm = "erase.topic.sois";
//   public String topicNm = "erase.topic.corpShipTimedWait";

    public testPublishishToActiveMQ() throws Exception {
        super();
    }


    public int published;
    private ActiveMQConnectionFactory connectionFactory = null;
    private Connection connection = null;
    private Session session = null;
    private MessageProducer producer = null;


    //  Copied from activeMQConfig.. do we need?
    String brokerUrl = "tcp://clearcap-edcw-l201.test.cloud.fedex.com:61616";
    //String brokerUrl = "tcp://erase-mon-l4-01.test.cloud.fedex.com:61616";   // level 4
    //   String brokerUrl = "(tcp://clearcap-edcw-l201.test.cloud.fedex.com:61616)?persistent=true&useJmx=false&schedulerSupport=true";

//    broker:(tcp://localhost:61616)?persistent=true&useJmx=false&schedulerSupport=true

    String username = "clearcap";
    String password = "clearcap";

    private java.sql.Connection dbConnection = null;

    //@PostConstruct
    public void setup() throws Exception {

    }

    private int insertMessageIdTable(long messageIdFromProperties) throws Exception {
        String logInfo = ".insertMessageIdTable(msgID:" + messageIdFromProperties + ") ";

        MsgId inData = new MsgId();
        inData.setMsgId(messageIdFromProperties);
        PreparedStatement statement = null;
        int rowCount = 0;
        try {
            statement = dbConnection.prepareStatement(insertSQL);
            statement.setLong(1, inData.getMsgId()); //MSG_ID
            rowCount = statement.executeUpdate();
            dbConnection.commit();
        } catch (SQLException ex) {
            log.warn(logInfo + "DB-73000 caught SQLException: " + ex);
            throw ex;
        } catch (Exception ex2) {
            log.warn(logInfo + "DB-73010 caught Exception: " + ex2);
            throw ex2;
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(logInfo + " complete ");
        }
        return rowCount;
    }

    public void closeConnections() throws Exception {
        log.info(this.topicNm + "ActiveMQPublisherBase closeConnections() ");
        try {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ActiveMQConnectionFactory connectionFactory() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        connectionFactory.setBrokerURL(brokerUrl);
        connectionFactory.setPassword(username);
        connectionFactory.setUserName(password);
        return connectionFactory;
    }

    public void startConnection() throws Exception {
        log.info(this.topicNm + "ActiveMQPublisherBase startConnection() TODO clean up");
        try {
            // Get connection factory
            connectionFactory = connectionFactory();//jmsTemplate.getConnectionFactory();
            // Get connection
            connection = connectionFactory.createConnection();
            connection.start();
            // JMS messages are sent and received using a Session. We will
            // create here a non-transactional session object. If you want
            // to use transactions you should set the first parameter to 'true'
            session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            // Create the topic to publish to
            Topic topic = session.createTopic(topicNm);
            producer = session.createProducer(topic);
            producer.setDeliveryMode(JmsProperties.DeliveryMode.PERSISTENT.getValue());

        } catch (Exception e) {
            throw e;

        }
    }

    private void connectToDatabase() throws Exception {
        String logInfo = "connectToDatabase() ";

        try {
            //TODO this is null.. how get past it?
            dbConnection = dataSource.getConnection();
            dbConnection.setAutoCommit(false);


        } catch (Exception ex) {
            log.error("connectToDatabase(), DB-1100: connect() failed " + ex, ex);
            throw ex;
        }
        if (log.isDebugEnabled()) {
            log.debug("connectToDatabase(), completed");
        }
    }

    private void closeConnectionToDatabase() throws RemoteException, Exception {
        String logInfo = "closeConnectionToDatabase() ";

        try {
            // Returns: true if the connection is closed; false if it's still open
            if (dbConnection != null && !dbConnection.isClosed()) {
                dbConnection.close();
            }
        } catch (Exception ex) {
            log.warn("closeConnectionToDatabase(), DB-1400: Close connection failed: " + ex, ex);
            log.warn("closeConnectionToDatabase(), DB Connection close failed,  Possibly leaked");
        }
        if (log.isDebugEnabled()) {
            log.debug("closeConnectionToDatabase() completed");
        }
    }

    public void sendIt(byte[] messagePayLoad, Map<String, Object> messageProperties, long delay) throws Exception {
        log.info(this.topicNm + "ActiveMQPublisherBase sendIt()");
        try {
            log.info("SendIt:()");
            BytesMessage bmsg = session.createBytesMessage();
            bmsg.clearBody();
            bmsg.writeBytes(messagePayLoad);
            if (messageProperties != null) {
                for (Map.Entry<String, Object> entry : messageProperties.entrySet()) {
                    //log.info("Key = " + entry.getKey() +", Value = " + entry.getValue());
                    String value = entry.getValue().toString();
                    bmsg.setObjectProperty(entry.getKey(), value);
                }
                //Set the Delay
                bmsg.setLongProperty("_AMQ_SCHED_DELIVERY", System.currentTimeMillis() + delay);
            }
            producer.send(bmsg);
            session.commit();


        } catch (Exception e) {
            log.error(this.topicNm + "activeMQPublisher.sendIt : {} ", e);
            throw e;
        }
    }

    /**
     * Entry point
     *
     * @throws Exception
     */
    public void doItUnitTest() throws Exception {
        System.out.println("doItUnitTest(), called");
        try {
            startConnection();
            //connectToDatabase();
            long delayOneSeconds = 1000;
            long delayOneMinute = 60000;
            long delay30Seconds = 30000;
            long delay = 0;
            for (int msgId = 100; msgId <= 200; msgId++) {
                //insertMessageIdTable(msgId);

                byte[] messagePayLoad = "test1 message".getBytes();
                Map<String, Object> messageProperties = new HashMap<String, Object>();
                messageProperties.put(MESSAGE_ID_PROPERTY, msgId);

//            switch(msgId) {
//                case 100:
//                    delay = delayOneMinute;
//                    break;
//                case 101:
//                    delay = delay30Seconds;
//                    break;
//                default:
//                    delay = 0;
//            }

                sendIt(messagePayLoad, messageProperties, delay);
                log.info("******** TESTER ******: Published message id: " + msgId + ", with MS delay of: " + delay + "*******");

            }
        } catch (Exception e) {
            log.error("failed:", e);
        } finally {
            closeConnections();
            //closeConnectionToDatabase();
        }
    }

    public static void main(String[] args) {
        testPublishishToActiveMQ testPublishishToActiveMQ = null;

        try {
            testPublishishToActiveMQ = new testPublishishToActiveMQ();
            testPublishishToActiveMQ.doItUnitTest();
        } catch (Exception ex) {
            System.out.println("exception: " + ex);
        }
    }
}
