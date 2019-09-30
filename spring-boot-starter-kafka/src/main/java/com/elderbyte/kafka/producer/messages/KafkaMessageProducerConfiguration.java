package com.elderbyte.kafka.producer.messages;

import com.elderbyte.kafka.producer.KafkaProducer;
import com.elderbyte.kafka.producer.KafkaProducerTx;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaMessageProducerConfiguration {

    @Bean
    public KafkaMessageProducer kafkaMessageProducer(KafkaProducer<Object,Object> elderKafkaProducer){
        return new KafkaMessageProducerImpl(elderKafkaProducer);
    }

    @Bean
    @ConditionalOnBean(name = "elderKafkaProducerTx")
    public KafkaMessageProducerTx kafkaMessageProducerTx(KafkaProducerTx<Object,Object> elderKafkaProducerTx){
        return new KafkaMessageProducerTxImpl(elderKafkaProducerTx);
    }
}
