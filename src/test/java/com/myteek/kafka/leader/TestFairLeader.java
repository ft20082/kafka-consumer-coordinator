package com.myteek.kafka.leader;

import com.myteek.kafka.client.KCClient;
import com.myteek.kafka.util.Common;
import com.myteek.kafka.util.TestCommon;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestFairLeader {

    private static final Logger log = LoggerFactory.getLogger(TestFairLeader.class);

    private static final Random random = new Random();

    private String connectString = "192.168.90.8:2181";

    @Test
    public void testElection() {

        int maxCurrentSize = 200;
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        CompletableFuture<Void>[] future = new CompletableFuture[maxCurrentSize];
        try {
            for (int i = 0; i < maxCurrentSize; i ++) {
                future[i] = CompletableFuture.supplyAsync(() -> {
                    Thread thread = Thread.currentThread();
                    log.info("current thread: {} start", thread);
                    try {
                        KCClient kcClient = new KCClient(connectString);
                        FairLeader fairLeader = new FairLeader(kcClient, new LeaderStatusListenerImpl());
                        fairLeader.election();

                        log.info("current thread: {}, leader status: {}", thread, fairLeader.getLeaderStatus());
                        int sleepMillis = random.nextInt(60000);
                        log.debug("current thread: {}, sleep millis: {}", thread, sleepMillis);
                        Common.sleepMillis(sleepMillis);
                        fairLeader.close();
                        kcClient.close();
                    } catch (Exception e) {
                        log.error("current thread: {}, error", thread, e);
                    }

                    log.info("current thread: {} close", thread);
                    return null;
                }, executorService);
            }

            CompletableFuture<Void> f = CompletableFuture.allOf(future);
            f.get();
        } catch (Throwable e) {
            e.printStackTrace();
        }

    }

}
