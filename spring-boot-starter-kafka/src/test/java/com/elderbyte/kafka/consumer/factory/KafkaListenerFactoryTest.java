package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.tests.SpringBootTestApp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

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
}