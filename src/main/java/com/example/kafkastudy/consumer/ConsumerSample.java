package com.example.kafkastudy.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;

@Slf4j
@Component
public class ConsumerSample {

    private final static String TOPIC_NAME="jiangzh-topic";

    /**
     * kafka消费者最最简单的使用
     */
    public void helloWorld() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.211.130:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // 自动提交ack
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // 自动提交的频率
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅哪几个Topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        // 循环拉取信息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            records.forEach(record -> {
                log.info("topic: {}, partition: {}, offset: {}, key: {}, value: {}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            });
        }
    }

    /**
     * 手动提交offset
     */
    public void commitedOffset() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.211.130:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 手动提交 ack
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // 手动提交这个参数就失效了
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        // 消费者订阅哪几个Topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        // 循环拉取消息
        //int count = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            records.forEach(record -> {
                log.info("topic: {}, partition: {}, offset: {}, key: {}, value: {}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            });
            log.info("获取的消息数: {}", records.count());
            //if(records.count() != 0) count++;
            //log.info("count的值: {}", count);
            // 一般情况下offset的提交也是一个批量的提交, 可以不传这个回调函数
            //if (count %2 ==0) {
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        offsets.forEach((k, v) -> {
                            log.info("ack callback topic: {}, partition: {}, offset: {}",
                                    k.topic(), k.partition(), v.offset());
                        });
                    }
                });
            //}

        }
    }

    /**
     * 手动提交offset,并且手动控制不同partition的消费和ack
     */
    public void commitedOffsetWithPartition() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.211.130:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 手动提交 ack
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // 手动提交这个参数就失效了
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // consumer订阅的topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            records.partitions().forEach(topicPartition -> {
                List<ConsumerRecord<String, String>> record = records.records(topicPartition);
                record.forEach(item -> {
                    log.info("topic: {}, partition: {}, offset: {}, key: {}, value: {}",
                            item.topic(), item.partition(), item.offset(), item.key(), item.value());
                });
                long lastOffset = record.get(record.size() - 1).offset();
                // 提交单个partition中的offset
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(topicPartition, new OffsetAndMetadata(lastOffset + 1));
                // 提交ack
                consumer.commitSync(offset);
                log.info("==================partition - {} end=================", topicPartition);
            });
        }
    }


    /***
     * 手动提交offset,并且手动控制partition,更高级
     */
    public void commitedOffsetWithPartition2() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.211.130:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 手动提交 ack
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // 手动提交这个参数就失效了
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 根据partition与topic创建
        // topic: jiangzh-topic, partition: 0
        TopicPartition topicPartition0 = new TopicPartition(TOPIC_NAME, 0);
        // topic: jiangzh-topic, partition: 1
        TopicPartition topicPartition1 = new TopicPartition(TOPIC_NAME, 1);

        // 消费订阅某个topic的某个partition
        consumer.assign(Collections.singleton(topicPartition1));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                log.info("topic: {}, partition: {}, offset: {}, key: {}, value: {}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
            // 提交ack
            consumer.commitAsync(new OffsetCommitCallback() {

                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    offsets.forEach((topicPartition, metadate) -> {
                        log.info("topic: {}, partition: {}, offset: {}",
                                topicPartition.topic(), topicPartition.partition(), metadate.offset());
                    });
                }
            });
        }
    }

    /**
     * 手动指定offset的起始位置，及手动提交offset
     */
    public void controlOffset() throws InterruptedException {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.211.130:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 手动提交 ack
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // 手动提交这个参数就失效了
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 手动提交offset需要自己去维护某一个partition的offset
        TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);

        // 消费者订阅某个分区
        consumer.assign(Collections.singletonList(topicPartition));

        while (true) {
            // 手动指定offset起始位置
            /*
                1、人为控制offset起始位置
                2、如果出现程序错误，重复消费一次
             */
            /*
                1、第一次从0消费【一般情况】
                2、比如一次消费了100条， offset置为101并且存入Redis
                3、每次poll之前，从redis中获取最新的offset位置
                4、每次从这个位置开始消费
             */
            consumer.seek(topicPartition, 130);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    log.info("topic: {}, partition: {}, offset: {}, key: {}, va;ue:{}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }

                long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                consumer.commitSync(offsets);
                log.info("=============partition - {} end================", partition);
            }

            //因为目前每次循环都是指定动50的位置开始消费, 所以会不停的循环, 我们加一个睡眠
            Thread.sleep(5000);
        }
    }

    /**
     * 流量控制 - 限流
     */
    public void controlPause() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.211.130:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 手动提交 ack
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // 手动提交这个参数就失效了
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

        // 消费订阅某个主题的哪些分区
        consumer.assign(Arrays.asList(p0, p1));

        // 设置一个限流的消费数, 消费到某个数值停止消费
        long totalNum = 10;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (TopicPartition partition : records.partitions()) {
                long num = 0;
                List<ConsumerRecord<String, String>> pRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : pRecords) {
                    System.out.printf("patition = %d , offset = %d, key = %s, value = %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                    /*
                        1、接收到record信息以后，去令牌桶中拿取令牌
                        2、如果获取到令牌，则继续业务处理
                        3、如果获取不到令牌， 则pause等待令牌
                        4、当令牌桶中的令牌足够， 则将consumer置为resume状态
                     */
                    num++;
                    if(record.partition() == 0){
                        if(num >= totalNum){
                            consumer.pause(Arrays.asList(p0));
                        }
                    }

                    /*if(record.partition() == 1){
                        if(num == 40){
                            consumer.resume(Arrays.asList(p0));
                        }
                    }*/
                }

                long lastOffset = pRecords.get(pRecords.size() -1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(partition,new OffsetAndMetadata(lastOffset+1));
                // 提交offset
                consumer.commitSync(offset);
                log.info("=============partition - {} end================", partition);
            }
        }
    }
}
