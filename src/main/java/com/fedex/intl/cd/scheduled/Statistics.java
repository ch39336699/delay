package com.fedex.intl.cd.scheduled;

import com.fedex.intl.cd.jms.publish.XLTNormalQueue;
import com.fedex.intl.cd.jms.publish.InternalPub;
import com.fedex.intl.cd.jms.receive.internal.InternalRecv;
import com.fedex.intl.cd.jms.receive.RecvJMSConsumer;
import com.fedex.intl.cd.utils.DelayedMessageStats;
import com.fedex.intl.cd.utils.TeamsNotifier;
import lombok.extern.slf4j.Slf4j;
import oracle.ucp.jdbc.PoolDataSource;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.json.simple.JSONObject;
import org.springframework.aop.target.CommonsPool2TargetSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Profile({"dev", "release", "volume", "production"})
@Component
public class Statistics {

    private static final long INTERVAL = 1000 * 60 * 1; // 1 min

    public static final String STATS_PREFIX = "ActiveMQ.Statistics.Destination";

    /**
     Queue string
     */
    public static final String QUEUE_ID = "BrokerMonitor";

    /**
     Queue list
     */
    private List <String> queues;


    @Autowired(required = false)
    TeamsNotifier teamsNotifier;

    @Autowired
    @Qualifier("oracleDataSource")
    private DataSource dataSource;

    @Autowired
    @Qualifier("TargetPool")
    CommonsPool2TargetSource processorPool;


    @Autowired
    DelayedMessageStats delayedMessageStats;

    @Autowired(required = false)
    private InternalRecv internalRecv;

    @Autowired
    InternalPub internalPub;

    @Autowired
    RecvJMSConsumer recvJMSConsumer;

    @Autowired
    XLTNormalQueue xltNormalQueue;

    @Autowired
    BrokerService service;

    public JSONObject stats;

    public int xltNormalMsgCount;
    String lastReceivedTime;
    String lastClearFactPublishedTime;
    String lastIMPublishedTime;

    //Every 2 min
    @Scheduled(fixedDelay = INTERVAL)
    public void job() {
        try {
            //init();
            printStats();
        } catch (Exception e) {
            log.error("Statistics.job(), Exception:", e);
        }
    }

    public void init() {
        if (lastReceivedTime == null) {
            lastReceivedTime = teamsNotifier.getCurrentTime();
        }
        if (lastClearFactPublishedTime == null) {
            lastClearFactPublishedTime = teamsNotifier.getCurrentTime();
        }
        if (lastIMPublishedTime == null) {
            lastIMPublishedTime = teamsNotifier.getCurrentTime();
        }
    }

    public void printStats() {

        //=====================================Check Pool Statistics==================================
        JSONObject poolStats = new JSONObject();
        try {
            if ((dataSource != null) && (((PoolDataSource) dataSource).getStatistics() != null)) {
                poolStats.put("borrowedConnectionsCnt", ((PoolDataSource) dataSource).getStatistics().getBorrowedConnectionsCount());
                poolStats.put("availableConnectionsCnt", ((PoolDataSource) dataSource).getStatistics().getAvailableConnectionsCount());
                poolStats.put("remainingPoolCapacityCnt", ((PoolDataSource) dataSource).getStatistics().getRemainingPoolCapacityCount());
                poolStats.put("totalConnectionsCnt", ((PoolDataSource) dataSource).getStatistics().getTotalConnectionsCount());
                poolStats.put("connectionsClosedCnt", ((PoolDataSource) dataSource).getStatistics().getConnectionsClosedCount());
                poolStats.put("connectionsCreatedCnt", ((PoolDataSource) dataSource).getStatistics().getConnectionsCreatedCount());
                poolStats.put("avgConnectionWaitTime", ((PoolDataSource) dataSource).getStatistics().getAverageConnectionWaitTime());
                poolStats.put("pendingRequestsCount", ((PoolDataSource) dataSource).getStatistics().getPendingRequestsCount());
                poolStats.put("abandonedConnectionsCount", ((PoolDataSource) dataSource).getStatistics().getAbandonedConnectionsCount());
                poolStats.put("averageConnectionWaitTime", ((PoolDataSource) dataSource).getStatistics().getAverageConnectionWaitTime());
                poolStats.put("cumulativeConnectionBorrowedCount", ((PoolDataSource) dataSource).getStatistics().getCumulativeConnectionBorrowedCount());
                poolStats.put("cumulativeConnectionWaitTime", ((PoolDataSource) dataSource).getStatistics().getCumulativeConnectionReturnedCount());
                poolStats.put("cumulativeConnectionUseTime", ((PoolDataSource) dataSource).getStatistics().getCumulativeConnectionUseTime());
                poolStats.put("cumulativeConnectionWaitTime", ((PoolDataSource) dataSource).getStatistics().getCumulativeConnectionWaitTime());
            }
        } catch (Exception e) {
            log.error("Statistics.printStats(), Exception getting Oracle DB pool statistics. Exception:", e);
        }
        try {
            if (internalRecv != null) {
                poolStats.put("XLTNormalTargetPoolActiveCount", internalRecv._available.size());
            }
        } catch (Exception ex) {
            log.error("Statistics.printStats(), Exception getting XLTNormalTargetPool statistics. Exception:", ex);
        }

       // log.info("Statistics.printStats(), XLTNormalTargetPool Active: {} Idle: {}", processorPool.getActiveCount(), processorPool.getIdleCount());

        //=====================================Check JMS Messages Received ==================================
       /* JSONObject jmsStats = new JSONObject();
        jmsStats.put("xltNormalMsgCount", xltNormalMsgCount);
        try {
            if ((recvExternalConsumer != null) && (recvExternalConsumer.queueName != null) && (recvExternalConsumer.queueName.trim().length() > 0)) {
                jmsStats.put("queueName", recvExternalConsumer.queueName);
                jmsStats.put("receivedCnt", recvExternalConsumer.receivedCnt);
                recvExternalConsumer.receivedCnt = 0;
            }
        } catch (Exception ex) {
            log.error("Statistics.printStats(), Exception getting recvExternalConsumer statistics. Exception:", ex);
        }

        //=====================================Check JMS Messages Published ==================================
        try {
            if ((xltNormalExternalProducer != null) && (xltNormalExternalProducer.topicName != null) && (xltNormalExternalProducer.topicName.trim().length() > 0)) {
                jmsStats.put("topicName", xltNormalExternalProducer.topicName);
                jmsStats.put("publishedCnt", xltNormalExternalProducer.publishedCnt);
                xltNormalExternalProducer.publishedCnt = 0;
            }
        } catch (Exception ex) {
            log.error("Statistics.printStats(), Exception getting xltNormalExternalProducer statistics. Exception:", ex);
        }*/
   /*     try {
            File myfile = service.getDataDirectoryFile();
            log.info("Statistics.printStats(), ======= DATA DIRECTORY ===== {}", myfile.getAbsolutePath());
          //  ActiveMQDestination destinations[] = service.getDestinations();
           // ActiveMQDestination brokerDestinations[] = service.getBroker().getDestinations();
            ActiveMQDestination brokerServiceDestinations[] = service.getBroker().getBrokerService().getDestinations();
*/

        // delayedMessageStats.printMessages();

          /*  Map <ObjectName, DestinationView> queueViews = service.getAdminView().getBroker().getQueueViews();
            for (Map.Entry <ObjectName, DestinationView> entry : queueViews.entrySet()) {
                DestinationView destView = entry.getValue();
                log.info("QueueViews Name: {} QueueSize: {} EnqueueCount: {} InFlightCount: {} AverageEnqueueTime: {} DequeueCount: {}",
                        destView.getName(), destView.getQueueSize(), destView.getEnqueueCount(), destView.getInFlightCount(), destView.getAverageEnqueueTime(), destView.getDequeueCount());
            }

            Map <ObjectName, DestinationView> topicViews = service.getAdminView().getBroker().getTopicViews();
            for (Map.Entry <ObjectName, DestinationView> entry : topicViews.entrySet()) {
                DestinationView destView = entry.getValue();
                log.info("QueueViews Name: {} QueueSize: {} EnqueueCount: {} InFlightCount: {} AverageEnqueueTime: {} DequeueCount: {}",
                        destView.getName(), destView.getQueueSize(), destView.getEnqueueCount(), destView.getInFlightCount(), destView.getAverageEnqueueTime(), destView.getDequeueCount());
            }*/
           /* if (destinations != null && destinations.length > 0) {
                for (int index = 0; index < destinations.length; index++) {
                    log.info("Statistics.printStats(), ======= FOUND DESTINATION ===== {}", destinations[index].getPhysicalName());
                }
            }*/
           /* if (brokerDestinations != null && brokerDestinations.length > 0) {
                for (int index = 0; index < brokerDestinations.length; index++) {
                    //if(brokerDestinations[index].getPhysicalName().equals("local-xltNormal")) {
                        DestinationStatistics view = getDestinationStatistics(service, brokerDestinations[index]);
                        log.info("====>Stats: name: " + brokerDestinations[index].getPhysicalName() + " size: " + view.getMessages().getCount() + ", enqueues: "
                                + view.getEnqueues().getCount() + ", dequeues: "
                                + view.getDequeues().getCount() + ", dispatched: "
                                + view.getMessages().getCount() + ", messages: "
                                + view.getDispatched().getCount() + ", inflight: "
                                + view.getInflight().getCount() + ", expiries: "
                                + view.getExpired().getCount());
                  //  }
                }
            }*/
           /* if (brokerServiceDestinations != null && brokerServiceDestinations.length > 0) {
                for (int index = 0; index < brokerServiceDestinations.length; index++) {
                    log.info("Statistics.printStats(), ======= FOUND BROKER SERVICE DESTINATION ===== {}", brokerServiceDestinations[index].getPhysicalName());
                }
            }
            if (stats != null) {
                log.info("Statistics.printStats()", kv("Stats", stats));
            }
        } catch (Exception ex) {
            log.error("Statistics.printStats(), Exception getting XLTNormalTargetPool statistics. Exception:", ex);
        }*/

    }

    public static DestinationStatistics getDestinationStatistics(BrokerService broker, ActiveMQDestination destination) {
        DestinationStatistics result = null;
        org.apache.activemq.broker.region.Destination dest = getDestination(broker, destination);
        if (dest != null) {
            result = dest.getDestinationStatistics();
        }
        return result;
    }

    public static org.apache.activemq.broker.region.Destination getDestination(BrokerService target, ActiveMQDestination destination) {
        org.apache.activemq.broker.region.Destination result = null;
        for (org.apache.activemq.broker.region.Destination dest : getDestinationMap(target, destination).values()) {
            if (dest.getName().equals(destination.getPhysicalName())) {
                result = dest;
                break;
            }
        }
        return result;
    }

    private static Map <ActiveMQDestination, org.apache.activemq.broker.region.Destination> getDestinationMap(BrokerService target,
                                                                                                              ActiveMQDestination destination) {
        RegionBroker regionBroker = (RegionBroker) target.getRegionBroker();
        if (destination.isTemporary()) {
            return destination.isQueue() ? regionBroker.getTempQueueRegion().getDestinationMap() :
                    regionBroker.getTempTopicRegion().getDestinationMap();
        }
        return destination.isQueue() ?
                regionBroker.getQueueRegion().getDestinationMap() :
                regionBroker.getTopicRegion().getDestinationMap();
    }


}
