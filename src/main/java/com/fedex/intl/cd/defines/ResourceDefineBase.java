package com.fedex.intl.cd.defines;

public class ResourceDefineBase implements java.io.Serializable {

    public static final boolean RETRY_MESSAGE_RESET_COUNTER = true;
    public static final boolean RETRY_REPLAY = false;

    //=================Message Header Properties================
    public static final String MSG_ID = "MSG_ID";

    public static final String UUID = "UUID";
    public static final String TRACK_ITEM_NBR = "TrackItemNbr";
    public static final String JMS_DESTINATION = "jms_destination";
    public static final String AMQ_SCHEDULED_DELAY = "AMQ_SCHEDULED_DELAY";
    public static final String TYPE = "type";
    public static final String EXTERNAL_PUB = "externalPub";
    public static final String RULES_FILE = "rulesProcFile";
    public static final String STATE_TYPE = "StateType";
    public static final String MESSAGE_ID_RETRY_COUNT = "MESSAGE_ID_RETRY_COUNT";

    /**
     * ResourceDefine - Constructor
     */
    public ResourceDefineBase() {
    }

}
