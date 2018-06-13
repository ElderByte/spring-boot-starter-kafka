package com.elderbyte.kafka.consumer.factory.listeners;

import com.elderbyte.kafka.consumer.processing.ManagedProcessor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener;

import java.util.List;

public class ManagedBatchRawListener<K,V> implements BatchConsumerAwareMessageListener<byte[], byte[]> {

    private final ManagedProcessor<K,V> managedProcessor;

    public ManagedBatchRawListener(
            ManagedProcessor<K,V> managedProcessor
    ){
        this.managedProcessor = managedProcessor;
    }

    @Override
    public void onMessage(List<ConsumerRecord<byte[], byte[]>> data, Consumer<?, ?> consumer) {
        this.managedProcessor.processMessages(data, null, consumer);
    }
}
