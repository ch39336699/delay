package com.fedex.intl.cd.utils;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import com.fedex.intl.cd.configuration.AppConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.Collections.singletonList;
import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Service
public class TeamsNotifier {

    @Autowired()
    public AppConfigs appConfigs;

    @Value("${application}")
    String application;

    @Value("${msTeams.environment}")
    String environment;

    @Value("${info.eai}")
    String eai;

    @Value("${msTeams.webhook}")
    String webhook;

    private String alertLastSetTime = "0";
    private static boolean sendingAlert = false;

    private static String host = "internet.proxy.fedex.com";
    private static int port = 3128;

    //SEVERITY
    public static int INFO = 0;
    public static int WARNING = 1;
    public static int ERROR = 2;

    boolean msTeamAlerts;

    String infoImgURL = "https://media.istockphoto.com/vectors/blue-round-information-icon-button-on-a-white-background-vector-id1192499077?k=6&m=1192499077&s=170667a&w=0&h=arKlbn2MsQEJw4CkeJajFKZLxG0j1iTfgd_5OHwsNQI=";
    String warnImgURL = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSy8bTfj_xeWT1oEqyok61ny7AQ4C8i6G0UuAihqOHT0O0y-QuigJDsEj7Ud5F9dUiGq08&usqp=CAU";
    String errorImgURL = "https://icones.pro/wp-content/uploads/2021/05/symbole-d-avertissement-rouge.png";


    private static final String SEVERITY = "Severity";

    private static final String ENVIRONMENT = "Environment";

    private static final String ACTION = "Action";

    private static final String APPLICATION_KEY = "Application";

    private static final String DASHBOARD_URL_KEY = "Dashboard";

    private RestTemplate restTemplate;


    /**
     Summary section of every Teams message originating from Spring Boot Admin
     */
    private final String messageSummary = "DB Purge Notification";

    private WebClient webclient;

    public TeamsNotifier() {
        try {
            this.restTemplate = createNotifierRestTemplate();

        } catch (Exception ex) {
            log.error("TeamsNotifier(), Exception: ", ex);
        }
    }

    public String getAlertLastSetTime() {
        return alertLastSetTime;
    }

    public void setAlertLastSetTime() {
        alertLastSetTime = getCurrentTime();
    }


    public long getTimeDurationInMin(String prevTime, String currentTime) {
        try {
            if ((prevTime != null) && (prevTime.trim().length() > 1)) {
                SimpleDateFormat parseFormat = new SimpleDateFormat("HH:mm");
                Date prev = parseFormat.parse(prevTime);
                Date current = parseFormat.parse(currentTime);
                //https://stackoverflow.com/questions/42878963/difference-between-two-times-in-minutes-are-coming-as-negative
                //If your endTime is less than your startTime then endTime is on the next day
                //so then have to add 1 day/86 400 000 milliseconds.
                //1 day = 86 400 000 mill
                if (current.getTime() < prev.getTime()) {
                    long mills = ((current.getTime() + 86400000) - prev.getTime());
                    long minutes = mills / (1000 * 60);
                    return minutes;
                } else {
                    long mills = current.getTime() - prev.getTime();
                    long minutes = mills / (1000 * 60);
                    return minutes;
                }
            } else {
                return 0;
            }
        } catch (Exception ex) {
            log.error("TeamsNotifier.getTimeDurationInMin(), Exception: ", ex);
        }
        return 0;
    }

    public long getTimeSinceLastAlert() {
        return getTimeDurationInSec(this.getAlertLastSetTime(), this.getCurrentTime());
    }

    public long getTimeDurationInSec(String prevTime, String currentTime) {
        try {
            if ((prevTime == "0") || (currentTime == "0")) {
                return 0;
            }
            if ((prevTime != null) && (prevTime.trim().length() > 1)) {
                SimpleDateFormat parseFormat = new SimpleDateFormat("HH.mm.ss");
                Date prev = parseFormat.parse(prevTime);
                Date current = parseFormat.parse(currentTime);
                //https://stackoverflow.com/questions/42878963/difference-between-two-times-in-minutes-are-coming-as-negative
                //If your endTime is less than your startTime then endTime is on the next day
                //so then have to add 1 day/86 400 000 milliseconds.
                //1 day = 86 400 000 mill
                if (current.getTime() < prev.getTime()) {
                    long mills = ((current.getTime() + 86400000) - prev.getTime());
                    long seconds = mills / 1000;
                    return seconds;
                } else {
                    long mills = current.getTime() - prev.getTime();
                    long seconds = mills / 1000;
                    return seconds;
                }
            } else {
                return 0;
            }
        } catch (Exception ex) {
            log.error("TeamsNotifier.getTimeDurationInSec(), Exception: ", ex);
        }
        return 0;
    }

    public long getTimeDurationInMilliSec(String prevTime, String currentTime) {
        try {
            if ((prevTime != null) && (prevTime.trim().length() > 1)) {
                SimpleDateFormat parseFormat = new SimpleDateFormat("HH.mm.ss");
                Date prev = parseFormat.parse(prevTime);
                Date current = parseFormat.parse(currentTime);
                //https://stackoverflow.com/questions/42878963/difference-between-two-times-in-minutes-are-coming-as-negative
                //If your endTime is less than your startTime then endTime is on the next day
                //so then have to add 1 day/86 400 000 milliseconds.
                //1 day = 86 400 000 mill
                if (current.getTime() < prev.getTime()) {
                    long mills = ((current.getTime() + 86400000) - prev.getTime());
                    return mills;
                } else {
                    long mills = current.getTime() - prev.getTime();
                    return mills;
                }
            } else {
                return 0;
            }
        } catch (Exception ex) {
            log.error("TeamsNotifier.getTimeDurationInMilliSec(), Exception: ", ex);
        }
        return 0;
    }

    public String getCurrentTime() {
        try {
            SimpleDateFormat sdfAmerica = new SimpleDateFormat("HH.mm.ss");
            TimeZone tzInAmerica = TimeZone.getTimeZone("America/Denver");
            sdfAmerica.setTimeZone(tzInAmerica);
            return sdfAmerica.format(new Date());
        } catch (Exception ex) {
            log.error("TeamsNotifier.getCurrentTime(), Exception: ", ex);
        }
        return null;
    }

    /*
    Send MS Teams notification.
    */
    public void sendNotification(
            int severity,
            String title,
            String action,
            String alertMessage,
            String dashboardURL) {
        Message message = null;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String webhookUrl = null;
        try {
            readConfig();
            log.info("TeamsNotifier.sendNotification(), sendingAlert: {} msTeamAlerts: {}", sendingAlert, msTeamAlerts);
            if (sendingAlert || !msTeamAlerts) {
                //Already in the process of sending an alert.
                return;
            }
            sendingAlert = true;
            setAlertLastSetTime();
            message = createMessage(severity, title, action, alertMessage, dashboardURL);
            postToWebHook(message);
            JSONObject eventLogObj = new JSONObject();
            eventLogObj.put("title", title);
            eventLogObj.put("alertMessage", alertMessage);
            eventLogObj.put("application", application);
            eventLogObj.put("severity", severity);
            eventLogObj.put("webhookUrl", webhookUrl);
            log.info("Notification Message: {} ", message, kv("metadata", eventLogObj));
        } catch (Exception ex) {
            log.error("TeamsNotifier.sendNotification(), Exception: ", ex);
        } finally {
            sendingAlert = false;
        }
    }

    private Message createMessage(int severity,
                                  String title,
                                  String action,
                                  String activitySubtitle,
                                  String dashboardURL) {
        List <Fact> facts = new ArrayList <>();
        String themeColor = "6db33f";
        String imgURL = infoImgURL;
        if (severity == this.ERROR) {
            facts.add(new Fact(SEVERITY, "Severe"));
            facts.add(new Fact(ACTION, action));
            themeColor = "f2003c"; //Red
            imgURL = errorImgURL;
        } else if (severity == this.WARNING) {
            facts.add(new Fact(SEVERITY, "Warning"));
            facts.add(new Fact(ACTION, action));
            themeColor = "fff780"; //Yellow
            imgURL = warnImgURL;
        } else {
            facts.add(new Fact(SEVERITY, "Informational"));
            facts.add(new Fact(ACTION, action));
            imgURL = infoImgURL;
        }
        facts.add(new Fact(ENVIRONMENT, this.environment));
        facts.add(new Fact(APPLICATION_KEY, this.application));
        if (dashboardURL != null) {
            facts.add(new Fact(DASHBOARD_URL_KEY, dashboardURL));
        }

        Section section = Section.builder().activitySubtitle(activitySubtitle).facts(facts).activityImage(imgURL).build();
        return Message.builder().title(title).summary(messageSummary).themeColor(themeColor)
                .sections(singletonList(section)).build();
    }

    private void postToWebHook(Message message) {
        try {
            if (this.restTemplate == null) {
                this.restTemplate = createNotifierRestTemplate();
            }
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            this.restTemplate.postForEntity(new URI(this.webhook), new HttpEntity <Object>(message, headers), Void.class);
        } catch (Exception ex) {
            log.error("TeamsNotifier.postToWebHook(), Exception: ", ex);
        }
    }


    private String safeFormat(String format, Object... args) {
        try {
            return String.format(format, args);
        } catch (MissingFormatArgumentException ex) {
            log.warn("TeamsNotifier.safeFormat(), Exception while trying to format the message. Falling back by using the format string.", ex);
            return format;
        }
    }

    private static RestTemplate createNotifierRestTemplate() {
        RestTemplate restTemplate = new RestTemplate();
        try {
            if (host != null) {
                SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
                Proxy proxy = new Proxy(Proxy.Type.HTTP,
                        new InetSocketAddress(host, port));
                requestFactory.setProxy(proxy);
                restTemplate.setRequestFactory(requestFactory);
            }
        } catch (Exception ex) {
            log.error("TeamsNotifier.createNotifierRestTemplate(), Exception: ", ex);
        }
        return restTemplate;
    }

    @Data
    @Builder
    public static class Message {

        private final String summary;

        private final String themeColor;

        private final String title;

        @Builder.Default
        private final List <Section> sections = new ArrayList <>();

    }

    @Data
    @Builder
    public static class Section {
        private final String activityTitle;
        private final String activitySubtitle;
        private final String activityImage;

        @Builder.Default
        private final List <Fact> facts = new ArrayList <>();
    }

    @Data
    public static class Fact {
        private final String name;
        private final String value;
    }

    public void readConfig() throws Exception {
        try {
            String MSTEAM_ALERTS = getConfig("MSTEAM_ALERTS");
            if (MSTEAM_ALERTS != null) {
                try {
                    msTeamAlerts = Boolean.valueOf(MSTEAM_ALERTS);
                } catch (Exception ex) {
                    msTeamAlerts = false;
                }
            }
        } catch (Exception ex) {
            log.error("TeamsNotifier.readConfig(), The property MSTEAM_ALERTS was not set in the .yml file. Exception: ", ex);
            msTeamAlerts = false;
        }

    }

    /**
     <p>Description       : method to get the value from AppConfigs class </p>
     @param key String
     @return null
     @see com.fedex.intl.cd.configuration.AppConfigs
     @see com.fedex.intl.cd.utils.SpringContext
     */
    public String getConfig(String key) {
        try {
            if ((appConfigs != null) && (appConfigs.getGeneral() != null)) {
                return appConfigs.getGeneral().get(key);
            } else {
                log.warn("TeamsNotifier.getConfig(), Config is null or invalid");
            }
        } catch (Exception ex) {
            log.error("TeamsNotifier.getConfig(), Exception getting reference to configuration. Exception: ", ex);
        }
        return null;
    }

}
