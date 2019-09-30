package com.elderbyte.kafka.producer;

import com.elderbyte.kafka.producer.impl.KafkaProducerImpl;
import com.elderbyte.kafka.producer.impl.KafkaProducerTxImpl;
import com.elderbyte.kafka.producer.messages.KafkaMessageProducerConfiguration;
import com.elderbyte.kafka.producer.mock.KafkaProducerMock;
import com.elderbyte.kafka.producer.mock.KafkaProducerTxMock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
@Import({
        KafkaMessageProducerConfiguration.class
})
public class KafkaProducerConfiguration {

    /***************************************************************************
     *                                                                         *
     * Kafka client backed producers (real)                                    *
     *                                                                         *
     **************************************************************************/


    @Configuration
    @ConditionalOnProperty(value = "kafka.client.enabled", matchIfMissing = true)
    public static class KafkaProducerConfigurationReal {



        @Autowired(required = false)
        @Qualifier("kafkaTemplateTransactional")
        private KafkaTemplate<String, Object> kafkaOperationsTx;

        @Autowired(required = false)
        @Qualifier("elderKafkaTemplateTransactional")
        private KafkaTemplate<Object, Object> elderKafkaOperationsTx;

        /***************************************************************************
         *                                                                         *
         * Public API                                                              *
         *                                                                         *
         **************************************************************************/

        @Bean
        @DependsOn("elderKafkaNewTopicCreator")
        @Primary
        public KafkaProducer<String, Object> kafkaProducer(KafkaTemplate<String, Object> kafkaOperations){
            return new KafkaProducerImpl<>(kafkaOperations);
        }

        @Bean
        @ConditionalOnProperty("kafka.client.producer.transaction.id")
        public KafkaProducerTx<String, Object> kafkaProducerTx(){
            return new KafkaProducerTxImpl<>(kafkaOperationsTx);
        }

        /***************************************************************************
         *                                                                         *
         * Public API                                                              *
         *                                                                         *
         **************************************************************************/

        @Bean
        @DependsOn("elderKafkaNewTopicCreator")
        @Primary
        public KafkaProducer<Object, Object> elderKafkaProducer(KafkaTemplate<Object, Object> kafkaOperations){
            return new KafkaProducerImpl<>(kafkaOperations);
        }

        @Bean
        @ConditionalOnProperty("kafka.client.producer.transaction.id")
        public KafkaProducerTx<Object, Object> elderKafkaProducerTx(){
            return new KafkaProducerTxImpl<>(elderKafkaOperationsTx);
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

        /***************************************************************************
         *                                                                         *
         * Public API                                                              *
         *                                                                         *
         **************************************************************************/

        @Bean
        @DependsOn("elderKafkaNewTopicCreator")
        @Primary
        public KafkaProducer<Object, Object> elderKafkaProducer(){
            return new KafkaProducerMock<>();
        }

        @Bean
        public KafkaProducerTx<Object, Object> elderKafkaProducerTx(){
            return new KafkaProducerTxMock<>();
        }

    }

}
