package com.example.kafkastudy.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ConsumerApplicationTest {

    @Autowired
    ConsumerSample consumerSample;

    @Test
    public void helloWorld() {
        consumerSample.helloWorld();
    }

    @Test
    public void commitedOffset() {
        consumerSample.commitedOffset();
    }

    /**
     * 测试同一个topic不同partition
     */
    @Test
    public void commitedOffsetWithPartition() {
        consumerSample.commitedOffsetWithPartition();
    }

    @Test
    public void commitedOffsetWithPartition2() {
        consumerSample.commitedOffsetWithPartition2();
    }

    @Test
    public void controlOffset() throws InterruptedException {
        consumerSample.controlOffset();
    }

    @Test
    public void controlPause() {
        consumerSample.controlPause();
    }
}
