package com.elderbyte.kafka.producer;

import com.elderbyte.kafka.config.KafkaClientConfig;
import com.elderbyte.kafka.serialisation.SpringKafkaJsonSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnProperty(value = "kafka.client.producer.enabled", havingValue = "true", matchIfMissing = true)
public class DefaultJsonKafkaTemplateConfiguration {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    @Autowired
    private KafkaClientConfig config;

    @Autowired
    private SpringKafkaJsonSerializer springKafkaJsonSerializer;

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Bean
    @Primary
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @ConditionalOnProperty("kafka.client.producer.transaction.id")
    @Bean("kafkaTemplateTransactional")
    public KafkaTemplate<String, Object> kafkaTemplateTransactional() {
        var factory = producerFactory();
        config.getProducerTransactionId().ifPresent(tid -> factory.setTransactionIdPrefix(tid));
        return new KafkaTemplate<>(factory);
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private DefaultKafkaProducerFactory<String, Object> producerFactory() {
        var factory = new DefaultKafkaProducerFactory<String, Object>(producerConfigs());
        factory.setKeySerializer(new StringSerializer());
        factory.setValueSerializer(springKafkaJsonSerializer);
        return factory;
    }

    private Map<String, Object> producerConfigs() {
        var props = new HashMap<String, Object>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaServers());
        // See https://kafka.apache.org/documentation/#producerconfigs for more properties
        return props;
    }

}
