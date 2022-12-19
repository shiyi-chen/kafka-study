package com.example.kafkastudy.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.*;

@Slf4j
public class ConsumerRecordThreadSample {

    private final static String TOPIC_NAME = "jiangzh-topic";

    private final static String GROUP_ID = "test";

    private final static String BROKER_LIST = "192.168.211.130:9092";

    // Consumer处理
    public static class ConsumerExecutor {
        private final KafkaConsumer<String, String> consumer;
        private ExecutorService executors;

        public ConsumerExecutor(String brokerList, String groupId, String topic) {
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // 自动提交 ack
            props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); // 手动提交这个参数就失效了
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(Collections.singletonList(topic));
        }

        public void execute(int workerNum) {
            this.executors = new ThreadPoolExecutor(
                    workerNum,
                    workerNum,
                    0L, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );

            while (true) {
                ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, String> record : records) {
                    this.executors.execute(new ConsumerRecordWorker(record));
                }
            }
        }

        public void shutdown() {
            if (consumer != null) {
                consumer.close();
            }
            if (executors != null) {
                executors.shutdown();
            }
            try {
                if (!executors.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.out.println("Timeout.... Ignore for this case");
                }
            } catch (InterruptedException ignored) {
                System.out.println("Other thread interrupted this shutdown, ignore for this case.");
                Thread.currentThread().interrupt();
            }
        }
    }

    public static class ConsumerRecordWorker implements Runnable {

        private ConsumerRecord<String, String> record;

        public ConsumerRecordWorker(ConsumerRecord<String, String> record) {
            this.record = record;
        }

        @Override
        public void run() {
            // 真正的数据操作在这里
            log.info("thread: {}, topic: {}, partition: {}, offset: {}, key {}, value {}",
                    Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(),
                    record.key(), record.value()
            );
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ConsumerExecutor executor = new ConsumerExecutor(BROKER_LIST, GROUP_ID, TOPIC_NAME);
        int workerNum = 5;
        executor.execute(workerNum);

        // Thread.sleep(1000000);

        // executor.shutdown();
    }
}
