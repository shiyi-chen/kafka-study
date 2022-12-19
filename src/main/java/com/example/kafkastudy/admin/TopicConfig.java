package com.example.kafkastudy.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Collections;

@Slf4j
@Configuration
public class TopicConfig {

    private static final String TOPIC_NAME = "jz_topic";

    @Autowired
    AdminClient adminClient;

    @PostConstruct
    public void createTopic() {
        // 副本银子
        short rs = 1;
        // partition分区个数
        int partitionNums = 1;
        // 创建Topic
        NewTopic newTopic = new NewTopic(TOPIC_NAME, partitionNums, rs);
        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));
        log.info("topics: {}", topics);
    }
}
