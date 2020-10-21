package com.myteek.kafka.leader;

public enum LeaderStatus {

    /**
     * initial status
     */
    LOOKING,

    /**
     * leader status
     */
    LEADING,

    /**
     * follower status
     */
    FOLLOWING;

}
