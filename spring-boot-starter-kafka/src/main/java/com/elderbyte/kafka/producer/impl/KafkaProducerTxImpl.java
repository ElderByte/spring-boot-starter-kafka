package com.elderbyte.kafka.producer.impl;

import com.elderbyte.kafka.producer.KafkaMessage;
import com.elderbyte.kafka.producer.KafkaProducerTx;
import com.elderbyte.kafka.producer.impl.KafkaProducerImpl;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Collection;

public class KafkaProducerTxImpl<K,V> extends KafkaProducerImpl<K,V> implements KafkaProducerTx<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    /***************************************************************************
     *                                                                         *
     * Constructors                                                            *
     *                                                                         *
     **************************************************************************/

    public KafkaProducerTxImpl(KafkaTemplate<K,V> kafkaOperations){
        super(kafkaOperations);
        if(!kafkaOperations.isTransactional()) throw new IllegalArgumentException("You must provide a kafka template which supports transactions!");
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public void sendAllTransactionally(String topic, Collection<KafkaMessage<K, V>> kafkaMessages) {
        getKafkaOperations().executeInTransaction(t -> {
            kafkaMessages.forEach(m -> t.send(m.toRecord(topic)));
            return null;
        });
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
