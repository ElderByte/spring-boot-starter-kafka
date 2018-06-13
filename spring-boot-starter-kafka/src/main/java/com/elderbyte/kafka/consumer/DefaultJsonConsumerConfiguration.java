package com.elderbyte.kafka.consumer;

import com.elderbyte.kafka.config.KafkaClientConfig;
import com.elderbyte.kafka.serialisation.Json;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;

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

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/


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
