package com.myteek.kafka;

import com.myteek.kafka.base.Constants;
import com.myteek.kafka.base.TopicPartition;
import com.myteek.kafka.client.KCClient;
import com.myteek.kafka.leader.FairLeader;
import com.myteek.kafka.util.Common;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class KCCoordinator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KCCoordinator.class);

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final KCClient kcClient;

    private FairLeader fairLeader;

    private List<TopicPartition> topicPartitionList;

    private int topicPartitionListContentSize = 0;

    private String shareRootPath;

    private String coordinatorPath;

    private String progressPath;

    private String topicPartitionsPath;

    /**
     * timestamp, seconds
     */
    private long period;

    /**
     * timestamp, seconds
     */
    private long startTimestamp;

    /**
     * timestamp, seconds
     */
    private long currentMaxTimestamp;

    private KCStatus kcStatus = KCStatus.NORMAL;

    public KCCoordinator(String connectString, List<TopicPartition> topicPartitionList, long startTimestamp, long period) {
        this.kcClient = new KCClient(connectString);
        this.fairLeader = new FairLeader(kcClient, new KCLeaderStatusListener(this));
        this.topicPartitionList = topicPartitionList;
        this.topicPartitionListContentSize = topicPartitionList.stream()
                .map(item -> item.getPartitions().size())
                .reduce((a, b) -> a + b).get();
        this.shareRootPath = kcClient.getRootPath() + "/" + Constants.SHARE_ROOT_PATH;
        this.coordinatorPath = this.shareRootPath + "/" + Constants.SHARE_COORDINATOR_PATH;
        this.progressPath = this.shareRootPath + "/" + Constants.SHARE_PROGRESS_PATH;
        this.topicPartitionsPath = this.shareRootPath + "/" + Constants.SHARE_TOPIC_PARTITIONS_PATH;
        this.startTimestamp = startTimestamp;
        this.period = period;
        this.currentMaxTimestamp = startTimestamp + period;
        initRootPath();
    }

    private void initRootPath() {
        checkOrCreateZookeeperPath(shareRootPath, CreateMode.PERSISTENT);
        checkOrCreateZookeeperPath(coordinatorPath, CreateMode.EPHEMERAL);
        checkOrCreateZookeeperPath(progressPath, CreateMode.PERSISTENT);
        checkOrCreateZookeeperPath(topicPartitionsPath, CreateMode.EPHEMERAL);
    }

    private void checkOrCreateZookeeperPath(String path, CreateMode createMode) {
        Stat stat = kcClient.exists(path, null);
        if (stat == null) {
            kcClient.create(path, null, createMode);
        }
    }

    /**
     * initial
     */
    public void initialElection() {
        // election
        fairLeader.election();

        // initial current coordinator
        switch (fairLeader.getLeaderStatus()) {
            case LEADING:
                // save topic partitions data to zk
                kcClient.setData(topicPartitionsPath,
                        Common.toTopicPartitionsString(topicPartitionList).getBytes(Constants.CHARSET), -1);
                // set beginning period timestamp
                kcClient.setData(coordinatorPath, String.valueOf(startTimestamp).getBytes(Constants.CHARSET), -1);
                break;
            case FOLLOWING:
                // need do nothing
                break;
            default:
                log.error("error status in electing.");
        }
    }

    /**
     * set current max timestamp to share progress path
     * @return
     */
    public boolean currentPeriodComplete(List<TopicPartition> topicPartitions) {
        if (kcStatus == KCStatus.NORMAL) {
            kcStatus = KCStatus.SYNCING;
            boolean ret = true;
            for (TopicPartition topicPartition : topicPartitions) {
                for (Integer partition : topicPartition.getPartitions()) {
                    String progressTopicPartition = progressPath + "/" + topicPartition.getTopic() + "." + partition;
                    Stat stat = kcClient.exists(progressTopicPartition, null);
                    if (stat == null) {
                        kcClient.create(progressTopicPartition, new byte[0], CreateMode.EPHEMERAL);
                    }
                    boolean isSuccess = kcClient.setData(progressTopicPartition, String.valueOf(currentMaxTimestamp).getBytes(Constants.CHARSET), -1);
                    log.info("set progress topic partition path data, thread: {}, current topic:{} partition: {}, set data: {}, result: {}",
                            Thread.currentThread(), topicPartition.getTopic(), partition, currentMaxTimestamp, isSuccess);
                    if (!isSuccess) {
                        ret = false;
                    }
                }
            }


            switch (fairLeader.getLeaderStatus()) {
                case LEADING:
                    if (kcStatus == KCStatus.SYNCING) {
                        try {
                            // check share progress path data, check if can set next period timestamp
                            while (true) {
                                try {
                                    List<String> dirs = kcClient.getChildren(progressPath, null, null);
                                    if (dirs != null && dirs.size() == topicPartitionListContentSize) {
                                        int successNum = 0;
                                        for (String dir : dirs) {
                                            String currentItemDir = progressPath + "/" + dir;
                                            byte[] itemDirData = kcClient.getData(currentItemDir, null, null);
                                            if (itemDirData != null && itemDirData.length > 0) {
                                                int itemDirDataValue = Integer.valueOf(new String(itemDirData, Constants.CHARSET));
                                                log.info("leader get current item progress path data: thread: {}, current path:{}, data: {}, currentMaxTimestamp: {}",
                                                        Thread.currentThread(), currentItemDir, itemDirDataValue, currentMaxTimestamp);
                                                if (itemDirDataValue == currentMaxTimestamp) {
                                                    successNum ++;
                                                } else {
                                                    log.debug("progress path data error, " +
                                                            "currentItemDir: {}, itemDirData: {}, currentMaxTimestamp: {}",
                                                            currentItemDir, itemDirDataValue, currentMaxTimestamp);
                                                }
                                            } else {
                                                log.debug("get progress path data error. current item dir: {}", currentItemDir);
                                            }
                                        }
                                        if (successNum == topicPartitionListContentSize) {
                                            // update coordinator path next period timestamp
                                            long newCurrentMaxTimestamp = currentMaxTimestamp + period;
                                            log.info("leader thread change current max timestamp from {} to {}",
                                                    currentMaxTimestamp, newCurrentMaxTimestamp);
                                            currentMaxTimestamp = newCurrentMaxTimestamp;
                                            kcClient.setData(coordinatorPath,
                                                    String.valueOf(newCurrentMaxTimestamp).getBytes(Constants.CHARSET), -1);
                                            kcStatus = KCStatus.NORMAL;
                                            break;
                                        } else {
                                            log.info("leader status, not all topic partitions offset up to current max timestamp. thread: {}," +
                                                    " successNum: {}, " + "topic partition list content size: {}",
                                                    Thread.currentThread(), successNum, topicPartitionListContentSize);
                                        }
                                    } else {
                                        log.debug("current dir size not equal topic partitions list size. " +
                                                        "current dirs: {}, topic partitions list: {}",
                                                String.join(", ", dirs),
                                                Common.toTopicPartitionsString(topicPartitions));
                                    }
                                } catch (Throwable e) {
                                    log.error("get progress data error.", e);
                                }
                                Common.sleepMillis(50);
                            }
                        } catch (Throwable e) {
                            log.error("leading operate error.", e);
                        }
                    } else {
                        log.error("kc status error. need syncing status, but now is: {}, " +
                                        "topic partitions: {}, current thread: {}",
                                kcClient, topicPartitions.stream()
                                        .map(item -> item.toString())
                                        .collect(Collectors.joining(", ")), Thread.currentThread());
                    }
                    break;
                case FOLLOWING:
                    // trigger following get coordinator path data
                    if (kcStatus == KCStatus.SYNCING) {
                        try {
                            // monitor coordinator path data
                            while (true) {
                                try {
                                    byte[] data = kcClient.getData(coordinatorPath, null, null);
                                    if (data != null && data.length > 0) {
                                        int dataNum = Integer.valueOf(new String(data, Constants.CHARSET));
                                        log.debug("follower get coordinator path data: thread: {}, current path:{}, data: {}, currentMaxTimestamp: {}",
                                                Thread.currentThread(), coordinatorPath, dataNum, currentMaxTimestamp);
                                        if (dataNum == currentMaxTimestamp + period) {
                                            currentMaxTimestamp = dataNum;
                                            kcStatus = KCStatus.NORMAL;
                                            log.info("get coordinator data success. current thread: {}, " +
                                                            "current topic partitions: {}, current max timestamp: {}, " +
                                                            "new max timestamp: {}", Thread.currentThread(),
                                                    topicPartitions.stream()
                                                            .map(item -> item.toString())
                                                            .collect(Collectors.joining(", ")), currentMaxTimestamp, dataNum);
                                            break;
                                        } else {
                                            log.info("waiting for synchronized. current thread: {}, topic partitions: {}, " +
                                                    "dataNum: {} is equals to currentMaxTimestamp: {}", Thread.currentThread(),
                                                    Common.toTopicPartitionsString(topicPartitions), dataNum, currentMaxTimestamp);
                                        }
                                    } else {
                                        log.debug("get coordinator path data error. data: {}, value: {}",
                                                data, (data != null ? new String(data, Constants.CHARSET) : ""));
                                    }
                                } catch (Throwable e) {
                                    log.error("get coordinator data error. " +
                                                    "current Thread: {}, current topic partitions: {}",
                                            Thread.currentThread(), topicPartitions.stream()
                                                    .map(item -> item.toString())
                                                    .collect(Collectors.joining(", ")), e);
                                }
                                Common.sleepMillis(10);
                            }
                        } catch (Throwable e) {
                            log.error("following operate error.", e);
                        }
                    } else {
                        log.error("kc status error. need syncing status, but now is: {}, " +
                                        "topic partitions: {}, current thread: {}",
                                kcClient, topicPartitions.stream()
                                        .map(item -> item.toString())
                                        .collect(Collectors.joining(", ")), Thread.currentThread());
                    }
                    break;
                default:
                    log.error("current period complete get fair leader status error.");
            }
            return ret;
        } else {
            log.error("current thread is now in syncing status. waiting for syncing coordinator data.");
        }
        return false;
    }

    /**
     * check if the timestamp less than the current max timestamp
     * @param timestamp
     * @return
     */
    public boolean checkLimit(long timestamp) {
        return timestamp < currentMaxTimestamp;
    }

    @Override
    public void close() throws Exception {
        fairLeader.close();
        kcClient.close();
    }

    @Override
    public String toString() {
        return "KCCoordinator{" +
                "executorService=" + executorService +
                ", kcClient=" + kcClient +
                ", fairLeader=" + fairLeader +
                ", topicPartitionList=" + topicPartitionList +
                ", topicPartitionListContentSize=" + topicPartitionListContentSize +
                ", shareRootPath='" + shareRootPath + '\'' +
                ", coordinatorPath='" + coordinatorPath + '\'' +
                ", progressPath='" + progressPath + '\'' +
                ", topicPartitionsPath='" + topicPartitionsPath + '\'' +
                ", period=" + period +
                ", startTimestamp=" + startTimestamp +
                ", currentMaxTimestamp=" + currentMaxTimestamp +
                ", kcStatus=" + kcStatus +
                '}';
    }
}
