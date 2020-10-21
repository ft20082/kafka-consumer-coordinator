package com.myteek.kafka.client;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class KCWatcher implements Watcher {

    public static final Logger log = LoggerFactory.getLogger(KCWatcher.class);

    public static final CountDownLatch cdl = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent event) {
        cdl.countDown();
    }
}
