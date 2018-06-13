package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.consumer.configuration.AutoOffsetReset;
import com.elderbyte.kafka.consumer.processing.KafkaProcessorConfiguration;
import org.springframework.kafka.listener.config.ContainerProperties;


/**
 * The configuration of a managed listener / processor
 * @param <K>
 * @param <V>
 */
public interface KafkaListenerConfiguration<K,V> extends KafkaProcessorConfiguration<K,V> {

    ContainerProperties getContainerProperties();

    boolean isManualAck();

    boolean isBatch();

    AutoOffsetReset getAutoOffsetReset();

}

