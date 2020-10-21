package com.myteek.kafka.client;

import com.myteek.kafka.base.Constants;
import com.myteek.kafka.util.Common;
import com.myteek.kafka.util.TestCommon;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestKCClient {

    private static final Logger log = LoggerFactory.getLogger(TestKCClient.class);

    private String connectString = "192.168.90.8:2181";

    @Test
    public void testClient() {

        try {
            KCClient kcClient = new KCClient(connectString);
            Common.sleepMillis(10000);
            System.out.println("connect status is alive: " + kcClient.isAlive());
            System.out.println("connect status is connected: " + kcClient.isConnected());
            kcClient.close();
        } catch (Throwable e) {
            log.error("connect zookeeper error.", e);
        }

    }

    @Test
    public void testZkClient() {
        try {
            ZooKeeper zooKeeper = new ZooKeeper(connectString, Constants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT, new KCWatcher());
            int count = 0;
            while (zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
                System.out.println(count + ": " + zooKeeper.getState());
                System.out.println(count + ": " + zooKeeper.getSessionId());
                System.out.println(count + ": " + zooKeeper.getSessionTimeout());
                System.out.println(count + ": " + zooKeeper.toString());
                System.out.println(count + ": " + zooKeeper.getTestable());
                count ++;
                Common.sleepMillis(10);
            }
        } catch (Throwable e) {
            log.error("zk client error.", e);
        }

    }

}
