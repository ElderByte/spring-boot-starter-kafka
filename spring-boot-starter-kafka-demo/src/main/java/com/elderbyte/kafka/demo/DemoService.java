package com.elderbyte.kafka.demo;

import com.elderbyte.kafka.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class DemoService implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(DemoService.class);

    @Autowired
    private KafkaProducer<String, Object> producer;

    @Override
    public void afterPropertiesSet() throws Exception {
       // this.distributedHashMap = distributedHashMapKafkaService.distributedMapJson("test-group", "test-topic", AppDataDto.class);
    }
}
