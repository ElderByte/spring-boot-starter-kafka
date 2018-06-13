package com.elderbyte.kafka.consumer.factory.listeners;

import com.elderbyte.kafka.consumer.processing.ManagedProcessor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

public class ManagedAckBatchRawListener<K,V> implements BatchAcknowledgingConsumerAwareMessageListener<byte[], byte[]> {

    private final ManagedProcessor<K,V> managedProcessor;

    public ManagedAckBatchRawListener(
            ManagedProcessor<K,V> managedProcessor
    ){
        this.managedProcessor = managedProcessor;
    }

    @Override
    public void onMessage(List<ConsumerRecord<byte[], byte[]>> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        this.managedProcessor.processMessages(data, acknowledgment, consumer);
    }
}
