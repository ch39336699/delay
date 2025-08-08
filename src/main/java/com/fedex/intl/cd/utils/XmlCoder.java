package com.fedex.intl.cd.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.*;
import javax.xml.bind.util.ValidationEventCollector;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;

/**
 @author Sunil Menothu
 Create Date: June 29, 2009.
 <p>
 Description: A class to handles all XML encoding and decoding */
@Slf4j
public abstract class XmlCoder {
    private static final String m_whatVersion = "@(#) $RCSfile: XmlCoder.java,v $ $Revision: 38933 $ $Author: 837670 $ $Date: 2018-04-23 10:11:45 -0600 (Mon, 23 Apr 2018) $\n";
    // -----------------------------------------------------------------------------------------------------------------------------------------------
    private String thisClassName = "com.fedex.intl.cd.util.XmlCoder";
    // -----------------------------------------------------------------------------------------------------------------------------------------------

    public com.fedex.intl.cd.configuration.AppConfigs appConfigs;

    private JAXBContext jaxbContext = null;
    private Marshaller marshaller = null;
    private Unmarshaller unMarshaller = null;

    protected String packageName = "";

    // NOTE:  static hashMap for caching JAXBContext, these are
    //  expensive due to reflection and classloading deep within the
    //  JAXB impl,  do not try this at home
    private static final HashMap <String, JAXBContext> contextCache = new HashMap();

    // moved these elements to class-visible so that decodeValidationOFF() can see them:
    private String validateUnmarshall = "N";
    private String validateMarshall = "N";
    private Schema schema = null;

    ValidationEventCollector vec = new ValidationEventCollector();

    protected Resource getResource(String path) {
        try {
            ResourceService resourceService = SpringContext.getBean(ResourceService.class);
            if (resourceService != null) {
                return resourceService.getResource(path);
            }
        } catch (Exception ex) {
            log.error("XmlCoder.getResource(), Exception: ", ex);
        }
        return null;
    }

    protected String getConfig(String key) {
        try {
            if (appConfigs == null) {
                appConfigs = SpringContext.getBean(com.fedex.intl.cd.configuration.AppConfigs.class);
            }
            if (appConfigs != null) {
                return appConfigs.getGeneral().get(key);
            }
        } catch (Exception ex) {
            log.error("XmlCoder.getConfig(), Exception: ", ex);
        }
        return null;
    }


    public void init(String xsdFile, String _packageName) throws JAXBException {
        String xsdName = "NOT_FOUND";
        packageName = _packageName.trim();
        jaxbContext = contextCache.get(_packageName.trim());
        if (jaxbContext == null) {
            try {
                jaxbContext = JAXBContext.newInstance(_packageName.trim());
                log.debug("XmlCoder.init(), Adding {} to contextCache", _packageName);
                contextCache.put(_packageName, jaxbContext);
            } catch (Exception ex) {
                log.error("XmlCoder.init(), Exception: ", ex);
            }
        }
        try {
            validateMarshall = getConfig("XML_VALIDATE_MARSHALL");
            if (validateMarshall == null) {
                validateMarshall = "N";
            }
            validateUnmarshall = getConfig("XML_VALIDATE_UNMARSHALL");
            if (validateUnmarshall == null) {
                validateUnmarshall = "N";
            }
        } catch (Exception ex) {
            log.error("XmlCoder.init(), Getting config. Exception: ", ex);
        }
        Resource fileResource = this.getResource("classpath:" + xsdFile);
        if ((fileResource == null) || (!fileResource.exists())) {
            log.info("XmlCoder.init(), Config entry MISSING for packageName == {}, xml message validation will NOT OCCUR", packageName);
        } else {
            xsdName = fileResource.getFilename();
        }

        log.debug("XmlCoder.init(), packageName: {}, xsdName: {}, validateMarshal: {}, validateUnmarshall: {}", packageName, xsdName, validateMarshall, validateUnmarshall);

        SchemaFactory sf = null;
        if (!xsdName.equalsIgnoreCase("NOT_FOUND")) {
            sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            try {
                schema = sf.newSchema(fileResource.getFile());
            } catch (SAXException sae) {
                log.error("XmlCoder.init(), SAXException. Failed to create schema for: " + xsdName, sae);
                xsdName = "NOT_FOUND";
                validateMarshall = "N";
                validateUnmarshall = "N";
            } catch (IOException sae) {
                log.error("XmlCoder.init(), IOException. Failed to create schema for: " + xsdName, sae);
                xsdName = "NOT_FOUND";
                validateMarshall = "N";
                validateUnmarshall = "N";
            }
        } else {
            log.debug("XmlCoder.init(), xsdName is Not Found.");
        }

        marshaller = jaxbContext.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        if (validateMarshall.equalsIgnoreCase("Y")) {
            log.debug("XmlCoder.init(), Setting validation ON");
            marshaller.setSchema(schema);
        }
        unMarshaller = jaxbContext.createUnmarshaller();
        if (validateUnmarshall.equals("Y")) {
            unMarshaller.setSchema(schema);
        }

    }

    // ----------------------------------------------------------
    //  Decoding methods
    // ----------------------------------------------------------

    /**
     Decode an XML String
     */
    public Object decode(String xmlStr) throws Exception {
        log.trace("XmlCoder.decode(String), START");
        Object retVal = null;
        try {
            retVal = unMarshaller.unmarshal(new StreamSource(
                    new StringReader(xmlStr)));
            // throw new UnmarshalException("Error during conn.rollback(), msgType: ");
        } catch (Exception e) {
            // if we aren't validating to begin with, we're done here..
            if (validateUnmarshall.equals("Y")) {
                log.debug("XmlCoder.decode(String), Decode: retrying with Validation turned OFF");
                retVal = decodeValidationOFF(xmlStr);
                if (retVal == null) {
                    log.error("XmlCoder.decode(String), Decode failed on retry with validation OFF...");
                    throw e;
                } else {
                    log.debug("XmlCoder.decode(String), retry with validation OFF, succeeded...");
                }
            } else {
                log.warn("XmlCoder.decode(String), Coder validation for {} is OFF, no second pass decode attempted.", packageName);
            }
        }
        //log.debug("XmlCoder.decode(String), Decoded. packageName: {} value: {}", packageName, retVal);
        log.trace("XmlCoder.decode(String), END");
        return retVal;

    }// end decode

    /**
     Method to force decoding with schema validation turned off. not intended to be a Public method
     */
    protected Object decodeValidationOFF(String xmlStr) {
        log.trace("XmlCoder.decodeValidationOFF(), START");
        Object retVal = null;
        try {
            // turn off validation:
            unMarshaller.setSchema(null);
            // throw new UnmarshalException("Error during conn.rollback(), msgType: ");
            // try unmarshalling:
            retVal = unMarshaller.unmarshal(new StreamSource(new StringReader(xmlStr)));
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("XmlCoder.decodeValidationOFF(), Failed for:" + packageName + " Exception: ", e);
            }
        }
        // put validation back:
        unMarshaller.setSchema(schema);

        return retVal;
    }

    /**
     *
     */
    public Object decode(byte[] xmlByteArray) throws Exception {
        String xmlStr = new String(xmlByteArray);
        return decode(xmlStr);
    }


    /**
     Decode an XML String
     */
    public JAXBElement decode(org.w3c.dom.Node node, Class cls) throws Exception {
        log.trace("XmlCoder.decode(Node),  START");
        JAXBElement root = null;
        try {
            root = unMarshaller.unmarshal(node, cls);
        } catch (Exception e) {
            log.error("XmlCoder.decode(Node), Failed for:" + packageName + " Exception: ", e);
            throw e;
        }

        log.trace("XmlCoder.decode(Node),  END");
        return root;

    }// end decode


    // ----------------------------------------------------------
    //  Encoding methods
    // ----------------------------------------------------------

    /**
     Encode and XML Reference to a String
     */
    public byte[] encode(Object request) throws Exception, JAXBException {
        log.trace("XmlCoder.encode(), START, Marshalling validation: {}", validateMarshall);

        String retVal = null;

        try {
            StringWriter sw = new StringWriter();
            marshaller.marshal(request, sw);

            retVal = sw.getBuffer().toString();
        } catch (JAXBException e) {
            //  SAXParseException se = new SAXParseException(e.getMessage(), null, null, -1, -1, e);

            // log.error("XmlCoder.encode(),  Failed for: " + packageName + ", SAXParseException: ", se);

            // if we aren't validating to begin with, we're done here.
            // With Validation ON, we'll retry the encode with validation OFF in order to (attempt to) continue processing:
            if (validateMarshall.equals("Y")) {
                log.debug("XmlCoder.encode(), Retrying with Validation turned OFF");
                retVal = encodeValidationOFF(request);
                if (retVal == null) {
                    log.error("XmlCoder.encode(), Encode failed on retry with validation OFF...");
                    throw e;
                } else {
                    log.debug("XmlCoder.encode(), Retry with validation OFF, succeeded...");
                }
            } else {
                throw e;
            }
        } catch (Exception e) {
            log.error("XmlCoder.encode(), Unknown error occurred for:" + packageName + " Exception: ", e);
            throw e;
        }

        log.debug("XmlCoder.encode(), Encoded. packageName: {} value: {} ", packageName, retVal);
        return retVal.getBytes();
    }

    /**
     Method to force encoding with schema validation turned off. not intended to be a Public method
     */
    protected String encodeValidationOFF(Object request) {
        log.trace("XmlCoder.encodeValidationOFF(), START");
        String retVal = null;
        try {
            // turn off validation:
            marshaller.setSchema(null);
            StringWriter sw = new StringWriter();
            marshaller.marshal(request, sw);
            retVal = sw.getBuffer().toString();

        } catch (Exception e) {
            log.error("XmlCoder.encodeValidationOFF(), Failed for:" + packageName, " Exception", e);
        }
        // put validation back:
        marshaller.setSchema(schema);

        return retVal;
    }
}
