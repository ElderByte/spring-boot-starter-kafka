package com.elderbyte.kafka.config;

import com.elderbyte.kafka.config.admin.DefaultKafkaAdminConfiguration;
import com.elderbyte.kafka.config.consumer.DefaultJsonConsumerConfiguration;
import com.elderbyte.kafka.config.producer.DefaultJsonProducerConfiguration;
import com.elderbyte.kafka.serialisation.SpringKafkaJsonDeserializer;
import com.elderbyte.kafka.serialisation.SpringKafkaJsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ConditionalOnProperty(value = "kafka.client.enabled", havingValue = "true", matchIfMissing = true)
@Import( {
        DefaultKafkaAdminConfiguration.class,
        DefaultJsonProducerConfiguration.class,
        DefaultJsonConsumerConfiguration.class
})
public class KafkaStarterAutoConfiguration {

    @Autowired
    private ObjectMapper mapper;

    @Bean
    public KafkaClientConfig kafkaClientConfig(){
        return new KafkaClientConfig();
    }

    @Bean
    public SpringKafkaJsonDeserializer springKafkaJsonDeserializer(){
        return new SpringKafkaJsonDeserializer(mapper);
    }

    @Bean
    public SpringKafkaJsonSerializer springKafkaJsonSerializer(){
        return new SpringKafkaJsonSerializer(mapper);
    }

}
