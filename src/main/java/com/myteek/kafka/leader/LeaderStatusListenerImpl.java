package com.myteek.kafka.leader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderStatusListenerImpl implements LeaderStatusListener {

    private static Logger log = LoggerFactory.getLogger(LeaderStatusListenerImpl.class);

    @Override
    public void statusChange(LeaderStatus status) {
        log.debug("current thread: {}, leader status: {}", Thread.currentThread(), status);
    }
}
