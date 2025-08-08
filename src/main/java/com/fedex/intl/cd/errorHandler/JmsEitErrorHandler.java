package com.fedex.intl.cd.errorHandler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.listener.adapter.ListenerExecutionFailedException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.util.ErrorHandler;

@Slf4j
public class JmsEitErrorHandler implements ErrorHandler {

    @Override
    public void handleError(Throwable t) {
        log.error("JmsEitErrorHandler.handleError(), Exception: ", t);
        if (t instanceof ListenerExecutionFailedException) {
            ListenerExecutionFailedException exception = (ListenerExecutionFailedException) t;
            Throwable cause = exception.getCause();
            if (cause instanceof MessageConversionException) {
                MessageConversionException converstionException = (MessageConversionException) cause;
                Object failedMessage = converstionException.getFailedMessage().getPayload();
                log.error("JmsEitErrorHandler.handleError(), MessageConversionException Message Type: {}", failedMessage.getClass());
                log.error("JmsEitErrorHandler.handleError(), MessageConversionException Failed message: {}", failedMessage);
            }
        }
    }

}
