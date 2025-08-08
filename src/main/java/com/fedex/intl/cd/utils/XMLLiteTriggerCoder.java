package com.fedex.intl.cd.utils;

import cimxmlmsgs.xmllitetrigger.XLTMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 <p>
 Description : Encodes/decodes the Broker message to/from XML.
 </p>
 <p>
 Copyright : Copyright (c) 2020
 </p>
 <p>
 Company : FedEx Services, Rocky Mountain Technology Center, Colorado Springs,
 CO
 </p>
 @author $Author: 393366 $
 @version $Revision: 41654 $ */
@Slf4j
@Service
public class XMLLiteTriggerCoder extends XmlCoder {

    /**

     */
    public XMLLiteTriggerCoder() throws Exception {
    }

    /**
     Initialize
     */
    @PostConstruct
    public void setup() throws Exception {
        try {
            this.init("schemas/XMLLiteTrigger.xsd", "cimxmlmsgs.xmllitetrigger");
        } catch (Exception e) {
            log.error("XMLLiteTriggerCoder.setup(), Exception: {}", e);
        }
    }

    /**
     Unmarshal the xmlByteArray into a JAXB created BrokerMessage object
     @return BrokerMessage
     */
    public XLTMessage decode(byte[] xmlByteArray) throws Exception {

        return (XLTMessage) super.decode(xmlByteArray);
    }

}
