package com.elderbyte.kafka.consumer.processing;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface ProcessingErrorHandler<K,V> {

    boolean handleError(List<ConsumerRecord<K, V>> records);

}
