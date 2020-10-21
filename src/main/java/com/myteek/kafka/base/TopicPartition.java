package com.myteek.kafka.base;

import java.util.List;
import java.util.stream.Collectors;

public class TopicPartition {

    private String topic;

    private List<Integer> partitions;

    public TopicPartition(String topic, List<Integer> partitions) {
        this.topic = topic;
        this.partitions = partitions;
    }

    public String getTopic() {
        return topic;
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    @Override
    public String toString() {
        return "TopicPartition{" +
                "topic='" + topic + '\'' +
                ", partitions=" + partitions.stream().map(item -> String.valueOf(item)).collect(Collectors.joining(", ")) +
                '}';
    }
}
