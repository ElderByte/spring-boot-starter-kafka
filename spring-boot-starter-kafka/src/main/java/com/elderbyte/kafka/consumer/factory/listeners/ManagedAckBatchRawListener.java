package com.elderbyte.kafka.consumer.factory.listeners;

import com.elderbyte.kafka.consumer.processing.Processor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

public class ManagedAckBatchRawListener<K,V> implements BatchAcknowledgingConsumerAwareMessageListener<byte[], byte[]> {

    public ManagedAckBatchRawListener(
            Processor<List<ConsumerRecord<K, V>>> processor
    ){

    }

    @Override
    public void onMessage(List<ConsumerRecord<byte[], byte[]>> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {

    }
}
