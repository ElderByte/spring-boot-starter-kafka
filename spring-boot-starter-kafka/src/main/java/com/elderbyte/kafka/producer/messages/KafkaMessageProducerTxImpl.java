package com.elderbyte.kafka.producer.messages;

import com.elderbyte.kafka.producer.KafkaProducerTx;
import org.springframework.kafka.support.SendResult;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

public class KafkaMessageProducerTxImpl extends KafkaMessageProducerImpl implements KafkaMessageProducerTx {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KafkaProducerTx<Object,Object> producerTx;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new MessageProducerTxImpl
     */
    public KafkaMessageProducerTxImpl(KafkaProducerTx<Object,Object> kafkaProducerTx) {
        super(kafkaProducerTx);
        this.producerTx = kafkaProducerTx;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    public  <
            MK,
            MV
            >
    List<CompletableFuture<SendResult<MK, MV>>> sendAllTransactionally(String topic, Collection<? extends MV> messageBodys) {
        var messages = messageBodys.stream()
                .map(this::fromMessage)
                .collect(toList());
        return (List<CompletableFuture<SendResult<MK, MV>>>)(Object)producerTx.sendAllTransactionally(topic, messages);
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
