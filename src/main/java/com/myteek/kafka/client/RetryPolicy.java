package com.myteek.kafka.client;

public interface RetryPolicy {

    boolean allowRetry(int retryCount);

}
