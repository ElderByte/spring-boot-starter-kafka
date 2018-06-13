package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.consumer.configuration.AutoOffsetReset;
import com.elderbyte.kafka.consumer.processing.Processor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.List;

/**
 * The configuration of a managed listener / processor
 * @param <K>
 * @param <V>
 */
public interface KafkaListenerConfiguration<K,V> {

    ContainerProperties getContainerProperties();

    boolean isManualAck();

    boolean isBatch();

    Deserializer<K> getKeyDeserializer();

    Deserializer<V> getValueDeserializer();

    Processor<List<ConsumerRecord<K, V>>> getProcessor();

    AutoOffsetReset getAutoOffsetReset();
}

