package com.elderbyte.kafka.producer.mock;

import com.elderbyte.kafka.producer.KafkaMessage;
import com.elderbyte.kafka.producer.KafkaProducerTx;

import java.util.Collection;

public class KafkaProducerTxMock<K,V> extends KafkaProducerMock<K,V> implements KafkaProducerTx<K,V> {

    @Override
    public void sendAllTransactionally(String topic, Collection<KafkaMessage<K, V>> kafkaMessages) {

    }
}
