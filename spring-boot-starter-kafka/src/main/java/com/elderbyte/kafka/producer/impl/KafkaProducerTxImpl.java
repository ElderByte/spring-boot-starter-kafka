package com.elderbyte.kafka.producer.impl;

import com.elderbyte.kafka.producer.KafkaMessage;
import com.elderbyte.kafka.producer.KafkaProducerTx;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

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
    public List<CompletableFuture<SendResult<K, V>>> sendAllTransactionally(String topic, Collection<KafkaMessage<K, V>> kafkaMessages) {
        return getKafkaOperations().executeInTransaction(t -> kafkaMessages.stream()
                .map(m -> t.send(m.toRecord(topic)))
                .map(ListenableFuture::completable)
                .collect(toList()));
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
