package com.example.kafkastudy.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ConsumerThreadSample {

    private final static String TOPIC_NAME="jiangzh-topic";

    public static class KafkaConsumerRunner implements Runnable {

        private final KafkaConsumer<String, String> consumer;

        // 是否结束监听
        private final AtomicBoolean closed = new AtomicBoolean(false);

        public KafkaConsumerRunner(int[] partitions, String topicName) {
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.211.130:9092");
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 手动提交 ack
            props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // 手动提交这个参数就失效了
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
            Arrays.stream(partitions).forEach(partition -> {
                topicPartitions.add(new TopicPartition(topicName, partition));
            });
            //TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.assign(topicPartitions);
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                while (!closed.get()) {
                    // 拉取消息
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                    // 处理每个分区的消息
                    Set<TopicPartition> partitions = records.partitions();
                    partitions.forEach(partition -> {
                        List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                        pRecord.forEach(record -> {
                            log.info("thread: {}, topic: {}, partition:{}, offset: {}, key: {}, value: {}",
                                    Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        });
                        // 当前这个partition消费到的offset位置
                        long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                        offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                        consumer.commitSync(offsets);
                    });
                }
            } catch (Exception e) {
                if(!closed.get()) {
                    throw e;
                }
            } finally {
                consumer.close();
            }
        }

        public void shutdown() {
            closed.set(true);
            consumer.wakeup();
        }

        public static void main(String[] args) {
            // 消费partition0
            KafkaConsumerRunner kafkaConsumerRunner0 = new KafkaConsumerRunner(new int[]{0}, TOPIC_NAME);
            // 消费partition1
            KafkaConsumerRunner kafkaConsumerRunner1 = new KafkaConsumerRunner(new int[]{1}, TOPIC_NAME);

            // 创建一个线程池来执行这两个任务
            ExecutorService executorService = Executors.newFixedThreadPool(2);
            // 执行消费者的监听
            executorService.execute(kafkaConsumerRunner0);
            executorService.execute(kafkaConsumerRunner1);
        }
    }
}
