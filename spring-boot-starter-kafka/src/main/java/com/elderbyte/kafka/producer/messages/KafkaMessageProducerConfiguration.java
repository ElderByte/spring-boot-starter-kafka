package com.elderbyte.kafka.producer.messages;

import com.elderbyte.kafka.producer.KafkaProducer;
import com.elderbyte.kafka.producer.KafkaProducerTx;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaMessageProducerConfiguration {

    @Bean
    public KafkaMessageProducer kafkaMessageProducer(KafkaProducer<Object,Object> producer){
        return new KafkaMessageProducerImpl(producer);
    }

    @Bean
    public KafkaMessageProducerTx kafkaMessageProducerTx(KafkaProducerTx<Object,Object> producer){
        return new KafkaMessageProducerTxImpl(producer);
    }
}
