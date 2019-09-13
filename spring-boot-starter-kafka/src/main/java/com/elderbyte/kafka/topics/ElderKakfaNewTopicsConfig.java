package com.elderbyte.kafka.topics;

import com.elderbyte.kafka.admin.AdminClientFactory;
import com.elderbyte.kafka.config.KafkaClientProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElderKakfaNewTopicsConfig {

    @ConditionalOnExpression("${kafka.client.enabled:true} and ${kafka.client.admin.enabled:true}")
    @Bean("elderKafkaNewTopicCreator")
    public ElderKafkaNewTopicCreator elderKafkaNewTopicCreator(
            @Autowired KafkaClientProperties properties,
            @Autowired AdminClientFactory factory){
        return new ElderKafkaNewTopicCreatorImpl(properties, factory);
    }

    @ConditionalOnExpression("${not kafka.client.enabled:true} or ${not kafka.client.admin.enabled:true}")
    @Bean("elderKafkaNewTopicCreator")
    public ElderKafkaNewTopicCreator elderKafkaNewTopicCreatorMock(){
        return new ElderKafkaNewTopicCreator() {};
    }

}
