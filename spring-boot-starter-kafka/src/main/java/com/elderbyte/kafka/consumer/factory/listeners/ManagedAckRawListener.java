package com.elderbyte.kafka.consumer.factory.listeners;

import com.elderbyte.kafka.consumer.factory.processor.ManagedProcessor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Collections;

public class ManagedAckRawListener<K,V> implements AcknowledgingConsumerAwareMessageListener<byte[], byte[]> {

    private final ManagedProcessor<K,V> managedProcessor;

    public ManagedAckRawListener(
            ManagedProcessor<K,V> managedProcessor
    ){
        this.managedProcessor = managedProcessor;
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        this.managedProcessor.processMessages(Collections.singletonList(data), acknowledgment, consumer);
    }
}
