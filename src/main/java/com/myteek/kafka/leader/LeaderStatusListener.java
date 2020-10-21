package com.myteek.kafka.leader;

public interface LeaderStatusListener {

    /**
     * status change event
     * @param status
     */
    void statusChange(LeaderStatus status);

}
