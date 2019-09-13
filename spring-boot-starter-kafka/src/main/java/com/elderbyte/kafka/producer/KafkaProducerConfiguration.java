package com.elderbyte.kafka.producer;

import com.elderbyte.kafka.producer.impl.KafkaProducerImpl;
import com.elderbyte.kafka.producer.impl.KafkaProducerTxImpl;
import com.elderbyte.kafka.producer.mock.KafkaProducerMock;
import com.elderbyte.kafka.producer.mock.KafkaProducerTxMock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class KafkaProducerConfiguration {

    /***************************************************************************
     *                                                                         *
     * Kafka client backed producers (real)                                    *
     *                                                                         *
     **************************************************************************/


    @Configuration
    @ConditionalOnProperty(value = "kafka.client.enabled", matchIfMissing = true)
    public static class KafkaProducerConfigurationReal {

        /***************************************************************************
         *                                                                         *
         * Fields                                                                  *
         *                                                                         *
         **************************************************************************/

        @Autowired
        private KafkaTemplate<String, Object> kafkaOperations;

        @Autowired(required = false)
        @Qualifier("kafkaTemplateTransactional")
        private KafkaTemplate<String, Object> kafkaOperationsTx;

        /***************************************************************************
         *                                                                         *
         * Public API                                                              *
         *                                                                         *
         **************************************************************************/

        @Bean
        @DependsOn("elderKafkaNewTopicCreator")
        @Primary
        public KafkaProducer<String, Object> kafkaProducer(){
            return new KafkaProducerImpl<>(kafkaOperations);
        }

        @Bean
        @ConditionalOnProperty("kafka.client.producer.transaction.id")
        public KafkaProducerTx<String, Object> kafkaProducerTx(){
            return new KafkaProducerTxImpl<>(kafkaOperationsTx);
        }
    }



    /***************************************************************************
     *                                                                         *
     * Mock producers                                                          *
     *                                                                         *
     **************************************************************************/


    @Configuration
    @ConditionalOnProperty(value = "kafka.client.enabled", havingValue = "false", matchIfMissing = false)
    public static class KafkaProducerConfigurationMock {

        /***************************************************************************
         *                                                                         *
         * Fields                                                                  *
         *                                                                         *
         **************************************************************************/

        /***************************************************************************
         *                                                                         *
         * Public API                                                              *
         *                                                                         *
         **************************************************************************/

        @Bean
        @DependsOn("elderKafkaNewTopicCreator")
        @Primary
        public KafkaProducer<String, Object> kafkaProducer(){
            return new KafkaProducerMock<>();
        }

        @Bean
        public KafkaProducerTx<String, Object> kafkaProducerTx(){
            return new KafkaProducerTxMock<>();
        }
    }

}
