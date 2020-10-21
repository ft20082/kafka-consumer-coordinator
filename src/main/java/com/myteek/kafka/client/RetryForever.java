package com.myteek.kafka.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryForever implements RetryPolicy {

    private static final Logger log = LoggerFactory.getLogger(RetryForever.class);

    private long sleepTimeMs;

    public RetryForever(long sleepTimeMs) {
        this.sleepTimeMs = sleepTimeMs;
    }

    @Override
    public boolean allowRetry(int retryCount) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            log.warn("sleep error.", e);
        }
        return true;
    }

}
