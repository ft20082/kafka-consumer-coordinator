package com.myteek.kafka.leader;

import com.myteek.kafka.base.Constants;
import com.myteek.kafka.client.KCClient;
import com.myteek.kafka.util.Common;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class FairLeader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FairLeader.class);

    private KCClient client;

    private String electionPath;

    private String electingPath;

    private String electionPathCurrent;

    private String watcherPath;

    private LeaderStatus leaderStatus = LeaderStatus.LOOKING;

    private FairWatcher fairWatcher = new FairWatcher(this);

    private LeaderStatusListener leaderStatusListener;

    private int retryNum = 0;

    public FairLeader(KCClient client, LeaderStatusListener leaderStatusListener) {
        this(client, leaderStatusListener, Constants.ELECTION_ROOT_PATH);
    }

    public FairLeader(KCClient client, LeaderStatusListener leaderStatusListener, String electionPath) {
        this.client = client;
        this.leaderStatusListener = leaderStatusListener;
        this.electionPath = client.getRootPath() + "/" + electionPath;
        this.electingPath = this.electionPath + "/" + Constants.ELECTING_PATH;
        initLeaderRootPath();
    }

    private void initLeaderRootPath() {
        Stat leadPathStat = client.exists(electionPath, null);
        if (leadPathStat == null) {
            client.create(electionPath, null, CreateMode.PERSISTENT);
        }
    }

    /**
     * election has two step, wather will trigger status change event
     * 1. create Ephemeral Sequence
     * 2. check current znode number, if is smallest, no watch anything. or watch current num - 1 znode
     */
    public void election() {
        if (client != null && client.isAlive()) {
            // check and initial current election path
            if (electionPathCurrent == null) {
                electionPathCurrent = client.create(electingPath, null, CreateMode.EPHEMERAL_SEQUENTIAL);
                log.info("electionPathCurrent initial from null to value: {}", electionPathCurrent);
            }

            // check electionPathCurrent is exists
            Stat electionPathCurrentStat = client.exists(electionPathCurrent, null);
            if (electionPathCurrentStat == null) {
                log.warn("electionPathCurrent is not exists in zookeeper, will trigger event change");
                statusChange(LeaderStatus.LOOKING);
                electionPathCurrent = client.create(electingPath, null, CreateMode.EPHEMERAL_SEQUENTIAL);
                log.info("new election path current value: {}", electionPathCurrent);
            }
            log.info("current election path current: {}", electionPathCurrent);
            // check electionPathCurrent in leaderPath path
            List<String> currentCandidates = client.getChildren(electionPath, null, null);
            if (currentCandidates != null && currentCandidates.size() > 0) {
                // sort
                List<String> newCurrentCandidates = currentCandidates.stream()
                        .map(item -> this.electionPath + "/" + item)
                        .sorted(Comparator.naturalOrder())
                        .collect(Collectors.toList());
                //Collections.sort(currentCandidates, Comparator.naturalOrder());
                newCurrentCandidates.forEach(item -> {
                    log.debug("current thread: {}, sorted current candidates: {}", Thread.currentThread(), item);
                });
                String lastZnodePath = null;
                String leaderZnode = null;
                for (String currentCandidate : newCurrentCandidates) {
                    // first candidate as leader
                    if (leaderZnode == null) {
                        leaderZnode = currentCandidate;
                    }
                    // add watcher, monitor previous znode exists
                    if (currentCandidate.equals(electionPathCurrent)) {
                        if (lastZnodePath != null) {
                            client.exists(lastZnodePath, fairWatcher);
                        }
                        break;
                    }
                    lastZnodePath = currentCandidate;
                }

                // status event change
                if (leaderZnode.equals(electionPathCurrent)) {
                    statusChange(LeaderStatus.LEADING);
                } else {
                    statusChange(LeaderStatus.FOLLOWING);
                }

                // clean current retry time
                retryNum = 0;
            } else {
                retryNum ++;
                log.error("get leader children error, current retry num: {}", retryNum);
                Common.sleepMillis(1000);
                // retry again
                election();
            }
        } else {
            log.warn("current thread: {}, kcClient is null or close. need re construct it.", Thread.currentThread());
        }
    }

    /**
     * trigger status change event
     * @param leaderStatus
     */
    private synchronized void statusChange(LeaderStatus leaderStatus) {
        log.info("current thread: {}, new election status: {}, current election status: {}, electionPathCurrent: {}",
                Thread.currentThread(), leaderStatus, this.leaderStatus, electionPathCurrent);
        if (this.leaderStatus != leaderStatus) {
            this.leaderStatus = leaderStatus;
            leaderStatusListener.statusChange(leaderStatus);
        }
    }

    public synchronized boolean isLeader() {
        return leaderStatus == LeaderStatus.LEADING;
    }

    public synchronized boolean isFollower() {
        return leaderStatus == LeaderStatus.FOLLOWING;
    }

    public synchronized LeaderStatus getLeaderStatus() {
        return leaderStatus;
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public String toString() {
        return "FairLeader{" +
                "client=" + client +
                ", electionPath='" + electionPath + '\'' +
                ", electingPath='" + electingPath + '\'' +
                ", electionPathCurrent='" + electionPathCurrent + '\'' +
                ", watcherPath='" + watcherPath + '\'' +
                ", leaderStatus=" + leaderStatus +
                ", fairWatcher=" + fairWatcher +
                ", leaderStatusListener=" + leaderStatusListener +
                ", retryNum=" + retryNum +
                '}';
    }
}
