package com.elderbyte.kafka.consumer.factory;

import org.springframework.kafka.listener.GenericMessageListenerContainer;


public interface ManagedListenerBuilder {
    <K,V> GenericMessageListenerContainer<byte[], byte[]> buildListenerContainer(KafkaListenerConfiguration<K,V> configuration);
}
