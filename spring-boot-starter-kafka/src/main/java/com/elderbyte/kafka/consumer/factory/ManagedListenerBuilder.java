package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.consumer.processing.Processor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.GenericMessageListenerContainer;

import java.util.List;

public interface ManagedListenerBuilder {

    <K,V> GenericMessageListenerContainer<byte[], byte[]> buildListener(KafkaListenerBuilder<K,V> builder, Processor<ConsumerRecord<K, V>> processor);

    <K,V> GenericMessageListenerContainer<byte[], byte[]> buildBatchListener(KafkaListenerBuilder<K,V> builder,Processor<List<ConsumerRecord<K, V>>> processor);
}
