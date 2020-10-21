package com.myteek.kafka.util;

import com.myteek.kafka.base.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Common {

    private static final Logger log = LoggerFactory.getLogger(Common.class);

    private Common() {}

    public static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Throwable e) {
            log.warn("thread sleep error.", e);
        }
    }


    public static String toTopicPartitionsString(List<TopicPartition> topicPartitions) {
        StringBuilder sb = new StringBuilder();
        topicPartitions.forEach(item -> {
            sb.append(item.getTopic()).append(":").append(item.getPartitions().stream().map(i -> i.toString()).collect(Collectors.joining(","))).append(";");
        });
        return sb.toString();
    }

    public static List<TopicPartition> fromTopicPartitionsString(String topicPartitionsItem) {
        return Arrays.stream(topicPartitionsItem.split(";")).map(item -> {
            String[] data = item.split(":");
            TopicPartition topicPartition = null;
            if (data.length == 2) {
                topicPartition = new TopicPartition(data[0], Stream.of(data[1].split(",")).map(i -> Integer.valueOf(i)).collect(Collectors.toList()));
            } else {
                log.error("topic partitions string data error. current string: {}, " +
                        "need format topic:partition0,partition1,partition2", item);
            }
            return topicPartition;
        }).filter(item -> item != null).collect(Collectors.toList());
    }

    public static long bytesToLong(byte[] data) {
        if (data == null || data.length != 8) {
            throw new RuntimeException("byte data error. data: " + data);
        }
        return (long) (
                ((0xff & data[0]) << 56) |
                ((0xff & data[1]) << 48) |
                ((0xff & data[2]) << 40) |
                ((0xff & data[3]) << 32) |
                ((0xff & data[4]) << 24) |
                ((0xff & data[5]) << 16) |
                ((0xff & data[6]) << 8) |
                ((0xff & data[7]) << 0)
        );
    }

    public static byte[] longToBytes(long data) {
        return new byte[] {
                (byte) ((data >> 56) & 0xff),
                (byte) ((data >> 48) & 0xff),
                (byte) ((data >> 40) & 0xff),
                (byte) ((data >> 32) & 0xff),
                (byte) ((data >> 24) & 0xff),
                (byte) ((data >> 16) & 0xff),
                (byte) ((data >> 8) & 0xff),
                (byte) ((data >> 0) & 0xff)
        };
    }

    public static int bytesToInt(byte[] data) {
        if (data == null || data.length != 4) {
            throw new RuntimeException("byte data error. data: " + data);
        }
        return (int) (
                ((0xff & data[0]) << 24) |
                ((0xff & data[1]) << 16) |
                ((0xff & data[2]) << 8) |
                ((0xff & data[3]) << 0)
        );
    }

    public static byte[] intToBytes(int data) {
        return new byte[] {
            (byte) ((data >> 24) & 0xff),
            (byte) ((data >> 16) & 0xff),
            (byte) ((data >> 8) & 0xff),
            (byte) ((data >> 0) & 0xff)
        };
    }






























}
