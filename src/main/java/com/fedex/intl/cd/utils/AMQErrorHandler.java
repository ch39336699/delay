package com.fedex.intl.cd.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.ErrorHandler;

@Slf4j
@Service
public class AMQErrorHandler implements ErrorHandler {
    @Override
    public void handleError(Throwable throwable) {
        log.error("AMQErrorHandler.handleError, {}", throwable.getCause().getMessage());
    }
}
