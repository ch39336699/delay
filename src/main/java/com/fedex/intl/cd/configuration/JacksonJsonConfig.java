package com.fedex.intl.cd.configuration;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import net.logstash.logback.decorate.JsonFactoryDecorator;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonJsonConfig implements JsonFactoryDecorator {

    @Override
    public JsonFactory decorate(JsonFactory factory) {
        ObjectMapper codec = (ObjectMapper) factory.getCodec();
        codec
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        return factory;
    }
}

