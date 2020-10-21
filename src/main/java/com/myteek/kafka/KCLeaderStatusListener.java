package com.myteek.kafka;

import com.myteek.kafka.leader.LeaderStatus;
import com.myteek.kafka.leader.LeaderStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KCLeaderStatusListener implements LeaderStatusListener {

    private static final Logger log = LoggerFactory.getLogger(KCLeaderStatusListener.class);

    private final KCCoordinator kcCoordinator;

    public KCLeaderStatusListener(KCCoordinator kcCoordinator) {
        this.kcCoordinator = kcCoordinator;
    }

    @Override
    public void statusChange(LeaderStatus status) {
        log.debug("current coordinator have new status: {}", status);
    }

}
