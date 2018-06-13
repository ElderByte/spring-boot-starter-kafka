package com.elderbyte.kafka.consumer.factory.listeners;

import com.elderbyte.kafka.consumer.factory.processor.ManagedProcessor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerAwareMessageListener;

import java.util.Collections;

public class ManagedRawListener<K,V> implements ConsumerAwareMessageListener<byte[], byte[]> {

    private final ManagedProcessor<K,V> managedProcessor;

    public ManagedRawListener(
            ManagedProcessor<K,V> managedProcessor
    ){
        this.managedProcessor = managedProcessor;
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> data, Consumer<?, ?> consumer) {
        this.managedProcessor.processMessages(Collections.singletonList(data), null, consumer);
    }
}
