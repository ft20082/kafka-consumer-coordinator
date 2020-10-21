package com.myteek.kafka.client;

public class RetryThreeTimes extends RetryTimes {

    public RetryThreeTimes() {
        super(3, 1000);
    }

}
