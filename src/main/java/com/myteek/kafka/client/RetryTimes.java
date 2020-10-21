package com.myteek.kafka.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class RetryTimes implements RetryPolicy {

    private static final Logger log = LoggerFactory.getLogger(RetryTimes.class);

    private int timesMax;
    private long sleepTimeMs;

    public RetryTimes(int timesMax, long sleepTimeMs) {
        this.timesMax = timesMax;
        this.sleepTimeMs = sleepTimeMs;
    }

    @Override
    public boolean allowRetry(int retryCount) {
        if (retryCount < timesMax) {
            try {
                Thread.sleep(sleepTimeMs);
            } catch (InterruptedException e) {
                log.warn("sleep error.", e);
            }
            return true;
        }
        return false;
    }

}
