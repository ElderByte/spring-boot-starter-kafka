package com.elderbyte.kafka.consumer.factory.listeners;

import com.elderbyte.kafka.consumer.processing.Processor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.support.Acknowledgment;

public class ManagedAckRawListener<K,V> implements AcknowledgingConsumerAwareMessageListener<byte[], byte[]> {

    public ManagedAckRawListener(
            Processor<ConsumerRecord<K, V>> processor
    ){

    }


    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {

    }
}
