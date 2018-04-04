package com.elderbyte.kafka.config;

import com.elderbyte.kafka.serialisation.SpringKafkaJsonDeserializer;
import com.elderbyte.kafka.serialisation.SpringKafkaJsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Import( { DefaultJsonProducerConfiguration.class, DefaultJsonConsumerConfiguration.class })
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

    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClientConfig().getKafkaServers());
        return new KafkaAdmin(configs);
    }
}
