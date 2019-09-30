package com.elderbyte.kafka.producer.messages;

import com.elderbyte.kafka.producer.AnnotationKafkaMessageBuilder;
import com.elderbyte.kafka.producer.KafkaMessage;
import com.elderbyte.kafka.producer.KafkaProducer;
import org.springframework.kafka.support.SendResult;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

public class KafkaMessageProducerImpl implements KafkaMessageProducer {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private KafkaProducer<Object,Object> producer;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new MessageProducerImpl
     */
    public KafkaMessageProducerImpl(KafkaProducer<Object,Object> producer) {
        if(producer == null) throw new IllegalArgumentException("producer");
        this.producer = producer;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    public
    <MK, MV> CompletableFuture<SendResult<MK, MV>> sendMessage(
            String topic,
            MV messageBody
    ) {
        var message = fromMessage(messageBody);
        return (CompletableFuture<SendResult<MK, MV>>)(Object)producer.send(topic, message);
    }

    public <MK,MV> List<CompletableFuture<SendResult<MK, MV>>> sendAllMessages(
            String topic,
            Collection<? extends MV> messageBodys
    ) {
        var messages = messageBodys.stream()
                .map(this::fromMessage)
                .collect(toList());
        return (List<CompletableFuture<SendResult<MK, MV>>> )(Object)producer.sendAll(topic, messages);
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    /**
     * Build a kafka message from an annotated message object.
     */
    protected KafkaMessage<Object, Object> fromMessage(Object messageObject){
        return AnnotationKafkaMessageBuilder.build(messageObject);
    }

}
