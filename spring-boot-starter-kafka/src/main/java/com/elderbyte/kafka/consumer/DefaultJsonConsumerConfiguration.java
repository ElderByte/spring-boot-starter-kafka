package com.elderbyte.kafka.consumer;

import com.elderbyte.kafka.config.KafkaClientConfig;
import com.elderbyte.kafka.serialisation.Json;
import com.elderbyte.kafka.serialisation.SpringKafkaJsonDeserializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnProperty(value = "kafka.client.consumer.enabled", havingValue = "true", matchIfMissing = true)
public class DefaultJsonConsumerConfiguration {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    public static final String JSON_BATCH_FACTORY = "kafkaBatchFactory";

    @Autowired
    private KafkaClientConfig config;

    @Autowired
    private SpringKafkaJsonDeserializer springKafkaJsonDeserializer;

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Default factory as json
     */
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Json>>
    kafkaListenerContainerFactory() {
        var factory = buildJsonContainerFactory(consumerFactory());
        return factory;
    }

    /**
     * Json batch factory
     */
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Json>>
    kafkaBatchFactory() {
        var factory = buildJsonContainerFactory(consumerFactory());
        factory.setBatchListener(true);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, Json> consumerFactory() {
        var factory = new DefaultKafkaConsumerFactory<String, Json>(consumerConfigs());
        factory.setKeyDeserializer(new StringDeserializer());
        factory.setValueDeserializer(springKafkaJsonDeserializer);

        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactoryStringString() {
        var factory = new DefaultKafkaConsumerFactory<String, String>(consumerConfigs());
        factory.setKeyDeserializer(new StringDeserializer());
        factory.setValueDeserializer(new StringDeserializer());

        return factory;
    }

    @Bean
    public ConsumerFactory<String, byte[]> consumerFactoryStringByte() {
        var factory = new DefaultKafkaConsumerFactory<String, byte[]>(consumerConfigs());
        factory.setKeyDeserializer(new StringDeserializer());
        factory.setValueDeserializer(new ByteArrayDeserializer());
        return factory;
    }

    @Bean
    public ConsumerFactory<byte[], byte[]> consumerFactoryByteByte() {
        var factory = new DefaultKafkaConsumerFactory<byte[], byte[]>(consumerConfigs());
        factory.setKeyDeserializer(new ByteArrayDeserializer());
        factory.setValueDeserializer(new ByteArrayDeserializer());
        return factory;
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaServers());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.isConsumerAutoCommit());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getConsumerAutoOffsetReset());
        config.getConsumerMaxPollRecords().ifPresent(max ->  props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, max));
        return props;
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private ConcurrentKafkaListenerContainerFactory<String, Json> buildJsonContainerFactory(ConsumerFactory<String, Json> consumerFactory){
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Json>();
        factory.setConsumerFactory(consumerFactory);
        config.getConsumerConcurrency().ifPresent(c -> factory.setConcurrency(c));
        config.getConsumerPollTimeout().ifPresent(t -> factory.getContainerProperties().setPollTimeout(t));

        factory.getContainerProperties().setAckMode(
                config.isConsumerAutoCommit()
                        ? AbstractMessageListenerContainer.AckMode.BATCH
                        : AbstractMessageListenerContainer.AckMode.MANUAL
        );


        return factory;
    }
}
