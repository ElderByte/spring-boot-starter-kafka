package com.elderbyte.kafka.producer;

import java.util.Collection;

/**
 * This producer supports transactional operations on top of the default ones.
 * @param <K>
 * @param <V>
 */
public interface KafkaProducerTx<K,V> extends KafkaProducer<K,V> {

    void sendAllTransactionally(String topic, Collection<KafkaMessage<K, V>> messages);

}
