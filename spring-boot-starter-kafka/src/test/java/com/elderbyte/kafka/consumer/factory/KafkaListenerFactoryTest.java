package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.consumer.configuration.AutoOffsetReset;
import com.elderbyte.kafka.tests.SpringBootTestApp;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringBootTestApp.class)
@TestPropertySource(properties = {
        "kafka.client.enabled=false",
})
public class KafkaListenerFactoryTest {

    @Autowired
    private KafkaListenerFactory listenerFactory;

    @Test
    public void start() {
        listenerFactory.start("test");
    }

    @Test
    public void config_type_switch() {
        KafkaListenerBuilder<String, KafkaListenerFactoryTest> builder = listenerFactory.start("test")
                    .consumerGroup("my-simple-group")
                    .stringKey()
                    .jsonValue(KafkaListenerFactoryTest.class)
                    .autoOffsetReset(AutoOffsetReset.latest);
    }

    @Test
    public void start_process() {
        listenerFactory.start("test")
                .consumerGroup("my-simple-group")
                .stringKey()
                .jsonValue(Object.class)
                .autoOffsetReset(AutoOffsetReset.latest)
                .startProcessBatch(records -> {
                    List<ConsumerRecord<String, Object>> myRecords = records;
                });

    }
}