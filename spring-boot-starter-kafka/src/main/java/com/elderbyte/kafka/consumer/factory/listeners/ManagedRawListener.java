package com.elderbyte.kafka.consumer.factory.listeners;

import com.elderbyte.kafka.consumer.processing.Processor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerAwareMessageListener;

public class ManagedRawListener<K,V> implements ConsumerAwareMessageListener<byte[], byte[]> {

    public ManagedRawListener(
            Processor<ConsumerRecord<K, V>> processor
    ){

    }


    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> data, Consumer<?, ?> consumer) {

    }
}
