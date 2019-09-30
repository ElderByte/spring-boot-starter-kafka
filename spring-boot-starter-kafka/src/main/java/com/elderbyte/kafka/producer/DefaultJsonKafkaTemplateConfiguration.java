package com.elderbyte.kafka.producer;

import com.elderbyte.kafka.config.KafkaClientProperties;
import com.elderbyte.kafka.serialisation.json.ElderKafkaJsonSerializer;
import com.elderbyte.kafka.serialisation.key.ElderCompositeKeySerializerGeneric;
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
    private KafkaClientProperties config;

    @Autowired
    private ElderKafkaJsonSerializer elderKafkaJsonSerializer;

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Bean("kafkaTemplate")
    @Primary
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @ConditionalOnProperty("kafka.client.producer.transaction.id")
    @Bean("kafkaTemplateTransactional")
    public KafkaTemplate<String, Object> kafkaTemplateTransactional() {
        var factory = producerFactory();
        var prodTid = config.getProducer().getTransaction().getId();
        if(prodTid != null){
            factory.setTransactionIdPrefix(prodTid);
        }
        return new KafkaTemplate<>(factory);
    }

    @Bean("elderKafkaTemplate")
    @Primary
    public KafkaTemplate<Object, Object> elderKafkaTemplate() {
        return new KafkaTemplate<>(elderProducerFactory());
    }

    @ConditionalOnProperty("kafka.client.producer.transaction.id")
    @Bean("elderKafkaTemplateTransactional")
    public KafkaTemplate<Object, Object> elderKafkaTemplateTransactional() {
        var factory = elderProducerFactory();
        var prodTid = config.getProducer().getTransaction().getId();
        if(prodTid != null){
            factory.setTransactionIdPrefix(prodTid);
        }
        return new KafkaTemplate<>(factory);
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private DefaultKafkaProducerFactory<Object, Object> elderProducerFactory() {
        var factory = new DefaultKafkaProducerFactory<Object, Object>(producerConfigs());
        factory.setKeySerializer(new ElderCompositeKeySerializerGeneric());
        factory.setValueSerializer(elderKafkaJsonSerializer);
        return factory;
    }

    private DefaultKafkaProducerFactory<String, Object> producerFactory() {
        var factory = new DefaultKafkaProducerFactory<String, Object>(producerConfigs());
        factory.setKeySerializer(new StringSerializer());
        factory.setValueSerializer(elderKafkaJsonSerializer);
        return factory;
    }

    private Map<String, Object> producerConfigs() {
        var props = new HashMap<String, Object>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getServers());
        // See https://kafka.apache.org/documentation/#producerconfigs for more properties
        return props;
    }

}
