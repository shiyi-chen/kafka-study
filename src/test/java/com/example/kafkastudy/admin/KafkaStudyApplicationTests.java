package com.example.kafkastudy.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@SpringBootTest
class KafkaStudyApplicationTests {

    @Autowired
    AdminClient adminClient;

    /**
     * 获取Topic列表
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    void topicList() throws ExecutionException, InterruptedException {
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);
        KafkaFuture<Set<String>> names = listTopicsResult.names();
        names.get().forEach(System.out::println);
    }

    /**
     * 删除 Topic
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    void deleteTopic() throws ExecutionException, InterruptedException {
        String topicName = "jiangzh";
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topicName));
        KafkaFuture<Void> all = deleteTopicsResult.all();
        all.get();
    }

    /**
     * 获取Topic的详细信息
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    void describeTopic() throws ExecutionException, InterruptedException {
        String topicName = "jz_topic";
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(topicName));
        Map<String, KafkaFuture<TopicDescription>> stringKafkaFutureMap = describeTopicsResult.topicNameValues();
        for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : stringKafkaFutureMap.entrySet()) {
            String name = entry.getKey();
            KafkaFuture<TopicDescription> describeFuture = entry.getValue();
            log.info("topicName: {}, describe: {}", name, describeFuture.get());
        }
    }

    /**
     * 获取Topic的配置信息
     */
    @Test
    void getTopicConfig() throws ExecutionException, InterruptedException {
        String topicName = "jz_topic";
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        DescribeConfigsResult configsResult = adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = configsResult.all().get();
        for (Map.Entry<ConfigResource, Config> entry : configResourceConfigMap.entrySet()) {
            log.info("key: {}, value: {}", entry.getKey(), entry.getValue());
        }
    }

    /**
     * 修改 kafka Topic 配置
     * 方式1 (标注废弃方法)
     */
    @Test
    void alterConfig1() throws ExecutionException, InterruptedException {
        String topicName = "jiangzh-topic";
        Map<ConfigResource, Config> configMap = new HashMap<>();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        ConfigEntry configEntry = new ConfigEntry("message.downconversion.enable", "false");
        Config config = new Config(Arrays.asList(configEntry));
        configMap.put(configResource, config);

        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(configMap);
        alterConfigsResult.all().get();
    }

    /**
     * 修改 kafka Topic 配置
     * 方式2
     */
    @Test
    void alterConfig2() throws ExecutionException, InterruptedException {
        String topicName = "jz_topic";
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        ConfigEntry configEntry = new ConfigEntry("message.downconversion.enable", "false");
        AlterConfigOp alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
        configs.put(configResource, Arrays.asList(alterConfigOp));

        AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(configs);
        alterConfigsResult.all().get();

        ExecutorService executorService = Executors.newFixedThreadPool(5);
    }

    /**
     * 将partition增加到2个
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    void incrPartitions() throws ExecutionException, InterruptedException {
        int partition = 2;
        String topicName = "jiangzh-topic";
        Map<String, NewPartitions> partitionsMap = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(partition);
        partitionsMap.put(topicName, newPartitions);
        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(partitionsMap);
        createPartitionsResult.all().get();
    }
}
