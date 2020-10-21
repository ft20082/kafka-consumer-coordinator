package com.myteek.kafka.base;

import jdk.internal.util.xml.impl.ReaderUTF8;

import java.nio.charset.Charset;

public class Constants {

    /**
     * zookeeper root path, can reset
     */
    public static final String DEFAULT_ZOOKEEPER_PATH = "/kafka-consumer-coordinator";

    /**
     * leader path
     */
    public static final String ELECTION_ROOT_PATH = "leader";

    /**
     * election path
     */
    public static final String ELECTING_PATH = "electing";

    /**
     * data share path
     */
    public static final String SHARE_ROOT_PATH = "share";

    /**
     * share data coordinator data
     */
    public static final String SHARE_COORDINATOR_PATH = "coordinator";

    /**
     * share data topic partitions data
     */
    public static final String SHARE_TOPIC_PARTITIONS_PATH = "topic-partitions";

    /**
     * share data progress data
     */
    public static final String SHARE_PROGRESS_PATH = "progress";

    /**
     * timestamp period
     */
    public static final Long DEFAULT_PERIOD = 0L;

    /**
     * zookeeper session timeout
     */
    public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 3000;

    /**
     * charset data
     */
    public static final Charset CHARSET = Charset.forName("UTF-8");

}
