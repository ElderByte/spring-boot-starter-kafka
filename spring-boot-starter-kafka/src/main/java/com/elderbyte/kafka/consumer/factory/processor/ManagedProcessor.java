package com.elderbyte.kafka.consumer.factory.processor;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

public interface ManagedProcessor<K,V> {


    void processMessages(List<ConsumerRecord<byte[], byte[]>> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer);

}
