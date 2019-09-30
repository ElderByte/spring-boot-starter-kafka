package com.elderbyte.kafka.producer.messages;

import org.springframework.kafka.support.SendResult;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public interface KafkaMessageProducer {

    <MK, MV> CompletableFuture<SendResult<MK, MV>> send(String topic, MV messageBody);

    <MK,MV> List<CompletableFuture<SendResult<MK, MV>>> sendAll(String topic, Collection<? extends MV> messageBodys);

}
