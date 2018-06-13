package com.elderbyte.kafka.consumer.factory.processor;

import com.elderbyte.kafka.consumer.factory.KafkaListenerConfiguration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

public class ManagedProcessorImpl<K,V> implements ManagedProcessor<K,V> {

    private final KafkaListenerConfiguration<K,V> configuration;

    public ManagedProcessorImpl(
            KafkaListenerConfiguration<K,V> configuration
    ){
      this.configuration = configuration;
    }


    @Override
    public void processMessages(List<ConsumerRecord<byte[], byte[]>> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {

        // transform

        // metrics

        // error handling

        // retry logic

        // health check ??

        throw new IllegalStateException("Not implemented yet!");
    }
}
