package com.example.kafkastudy.producer;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.ExecutionException;

@Slf4j
@SpringBootTest
public class ProducerApplicationTest {

    @Resource
    ProducerSample producerSample;

    /**
     * 异步发送消息
     */
    @Test
    void producerTest1() throws InterruptedException {
        producerSample.producerSend();
    }

    /**
     * 异步阻塞发送消息
     */
    @Test
    void producerTest2() throws ExecutionException, InterruptedException {
        producerSample.producerSyncSend();
    }

    /**
     * 异步发送带回调函数
     */
    @Test
    void incrPartitions() {
        producerSample.producerSendWithCallback();
    }

    /**
     * 异步回调+设置partition均衡
     */
    @Test
    void producerSendWithCallbackAndPartition() {
        producerSample.producerSendWithCallbackAndPartition();
    }


}
