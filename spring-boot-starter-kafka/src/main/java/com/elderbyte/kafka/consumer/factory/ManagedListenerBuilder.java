package com.elderbyte.kafka.consumer.factory;

import org.springframework.kafka.listener.MessageListenerContainer;


public interface ManagedListenerBuilder {
    <K,V> MessageListenerContainer buildListenerContainer(KafkaListenerConfiguration<K,V> configuration);
}
