package com.elderbyte.kafka.consumer;

import com.elderbyte.kafka.config.KafkaClientProperties;
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
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

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

    @Autowired
    private KafkaClientProperties config;

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/
    
    /**
     * Default factory as string
     */
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>
    kafkaListenerContainerFactory() {
        var factory = buildStringContainerFactory(consumerFactoryStringString());
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
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getServers());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getConsumer().isAutoCommit());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getConsumer().getAutoOffsetReset());

        var maxPollRecords = config.getConsumer().getMaxPollRecords();
        if(maxPollRecords != null){
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        }

        return props;
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private ConcurrentKafkaListenerContainerFactory<String, String> buildStringContainerFactory(ConsumerFactory<String, String> consumerFactory){
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);

        var consumer = config.getConsumer();

        if(consumer.getConcurrency() != null){
            factory.setConcurrency(consumer.getConcurrency());
        }

        if(consumer.getPollTimeout() != null){
            factory.getContainerProperties().setPollTimeout(consumer.getPollTimeout());
        }

        factory.getContainerProperties().setAckMode(
                config.getConsumer().isAutoCommit()
                        ? ContainerProperties.AckMode.BATCH
                        : ContainerProperties.AckMode.MANUAL
        );

        return factory;
    }
}
