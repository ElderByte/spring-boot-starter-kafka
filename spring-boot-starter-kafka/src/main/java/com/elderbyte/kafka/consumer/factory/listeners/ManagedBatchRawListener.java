package com.elderbyte.kafka.consumer.factory.listeners;

import com.elderbyte.kafka.consumer.processing.Processor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConsumerAwareMessageListener;

import java.util.List;

public class ManagedBatchRawListener<K,V> implements BatchConsumerAwareMessageListener<byte[], byte[]> {

    public ManagedBatchRawListener(
            Processor<List<ConsumerRecord<K, V>>> processor
    ){

    }

    @Override
    public void onMessage(List<ConsumerRecord<byte[], byte[]>> data, Consumer<?, ?> consumer) {

    }
}
