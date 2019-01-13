package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.consumer.configuration.AutoOffsetReset;
import com.elderbyte.kafka.consumer.processing.KafkaProcessorConfiguration;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;

import java.util.Collection;


/**
 * The configuration of a managed listener / processor
 * @param <K>
 * @param <V>
 */
public interface KafkaListenerConfiguration<K,V> extends KafkaProcessorConfiguration<K,V> {

    ContainerProperties getContainerProperties();

    boolean isManualAck();

    /**
     * Enable batch listener
     */
    boolean isBatch();

    AutoOffsetReset getAutoOffsetReset();

    /**
     * If enabled, will fail if configured topics are missing.
     */
    boolean failIfTopicsAreMissing();
}

