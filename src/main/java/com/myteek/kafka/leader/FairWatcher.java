package com.myteek.kafka.leader;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class FairWatcher implements Watcher {

    private final FairLeader fairLeader;

    public FairWatcher(FairLeader fairLeader) {
        this.fairLeader = fairLeader;
    }

    @Override
    public void process(WatchedEvent event) {
        fairLeader.election();
    }
}
