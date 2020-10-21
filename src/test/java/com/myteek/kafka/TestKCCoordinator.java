package com.myteek.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myteek.kafka.base.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestKCCoordinator {

    private static final Logger log = LoggerFactory.getLogger(TestKCCoordinator.class);

    private String connectString = "192.168.90.8:2181";

    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");

    @Test
    public void testTimeFormatter() {
        System.out.println(LocalDateTime.parse("10/Apr/2020:16:03:43 +0800", dateTimeFormatter).toEpochSecond(ZoneOffset.of("+0800")));
    }

    @Test
    public void testMain() {
        try {
            int partitionsNum = 3;
            ExecutorService executorService = Executors.newFixedThreadPool(10);
            CompletableFuture<Void>[] future = new CompletableFuture[partitionsNum];

            String topic = "test";
            try {

                for (int i = 0; i < partitionsNum; i ++) {
                    final int temp = i;
                    future[i] = CompletableFuture.supplyAsync(() -> {
                        Thread thread = Thread.currentThread();
                        log.info("current thread: {} start", thread);
                        try {
                            Properties properties = new Properties();
                            properties.setProperty("bootstrap.servers", "192.168.90.8:9092");
                            properties.setProperty("group.id", "test-topic-group");
                            properties.setProperty("enable.auto.commit", "false");
                            properties.setProperty("auto.offset.reset", "earliest");
                            properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                            properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

                            KCCoordinator kcCoordinator = new KCCoordinator(connectString,
                                    Arrays.asList(new TopicPartition(topic, Arrays.asList(0, 1, 2))), 1586505780, 60);
                            kcCoordinator.initialElection();

                            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

                            kafkaConsumer.assign(Arrays.asList(new org.apache.kafka.common.TopicPartition(topic, temp)));

                            while (true) {
                                try {
                                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                                        try {
                                            JSONObject jsonObject = JSON.parseObject(consumerRecord.value());
                                            String message = String.valueOf(jsonObject.get("message"));
                                            String messageArr[] = message.split("\\|");
                                            long timestamp = LocalDateTime.parse(messageArr[2].trim(), dateTimeFormatter).toEpochSecond(ZoneOffset.of("+0800"));

                                            // or put them in cache, check next 10 records timestamp
                                            if (kcCoordinator.checkLimit(timestamp)) {
                                                System.out.println("thread: " + Thread.currentThread() + ", topic: test, partition: " + temp + ", timestamp: " + timestamp + ", message: " + message);
                                            } else {
                                                // current timestamp is great than current max timestamp, waiting for synchronize
                                                log.info("current topic partition wait for synchronize, topic: {}, partition: {} start", topic, temp);
                                                kcCoordinator.currentPeriodComplete(Arrays.asList(new TopicPartition("test", Arrays.asList(temp))));
                                                log.info("current topic partition wait for synchronize, topic: {}, partition: {} end", topic, temp);
                                                // async kana offset commit
                                            }

                                        } catch (Throwable e) {
                                            log.error("parse json value error.", e);
                                        }

                                    }
                                } catch (Throwable e) {
                                    log.error("consumer data error, topic:{}, partitions: {}", topic, temp, e);
                                }

                            }

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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testKafka() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.90.8:9092");
        properties.setProperty("group.id", "test-topic-group");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        kafkaConsumer.assign(Arrays.asList(new org.apache.kafka.common.TopicPartition("test", 0)));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.value());
            }
        }
    }

}
