package com.elderbyte.kafka.producer.messages;

import org.springframework.kafka.support.SendResult;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface KafkaMessageProducerTx extends KafkaMessageProducer {

    <MK,MV>
    List<CompletableFuture<SendResult<MK, MV>>> sendAllTransactionally(String topic, Collection<? extends MV> messageBodys);

}
