package com.fedex.intl.cd.model;

import com.fedex.intl.cd.defines.ResourceDefineBase;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.UUID;

@Slf4j
@Getter
@Setter
@NoArgsConstructor
public class DelayedMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private String UUID;
    private String trackItemNumber;
    private long brokerInTime;
    private long brokerOutTime;
    private long oid;

    private int deliveryCnt;
    private String type;
    private String oidType;
    private String ruleset;
    private String rulesFile;
    private String AMQ_SCHEDULED_DELAY;

    private String MSG_ID;

    private String externalPub;

    public long getTimeQueued() {
        try {
            if (brokerOutTime > 0) {
                return brokerOutTime - brokerInTime;
            } else {
                return System.currentTimeMillis() - brokerInTime;
            }
        } catch (Exception ex) {
            log.error("DelayedMessage.getTimeQueued(), Exception:", ex);
        }
        return 0;
    }

    public JSONObject toJSON() {
        JSONObject message = new JSONObject();
        try {
            message.put("UUID:", UUID);
            message.put("brokerInTim:", brokerInTime);
            message.put("brokerOutTime:", brokerOutTime);
            message.put("timeQueued:", getTimeQueued());
            message.put("oid:", oid);
            if (type != null) {
                message.put("type:", type);
            }
            if (trackItemNumber != null) {
                message.put("trackItemNumber:", trackItemNumber);
            }
            if (ruleset != null) {
                message.put("ruleset:", ruleset);
            }
            if (rulesFile != null) {
                message.put("rulesFile:", rulesFile);
            }
            if (MSG_ID != null) {
                message.put("MSG_ID:", MSG_ID);
            }
            if (externalPub != null) {
                message.put("externalPub:", externalPub);
            }
            message.put("AMQ_SCHEDULED_DELAY:", AMQ_SCHEDULED_DELAY);
        } catch (Exception ex) {
            log.error("DelayedMessage.getTimeQueued(), Exception:", ex);
        }
        return message;
    }
}
