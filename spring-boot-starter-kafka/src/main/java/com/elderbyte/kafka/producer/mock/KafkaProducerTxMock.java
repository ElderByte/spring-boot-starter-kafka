package com.elderbyte.kafka.producer.mock;

import com.elderbyte.kafka.producer.KafkaMessage;
import com.elderbyte.kafka.producer.KafkaProducerTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class KafkaProducerTxMock<K,V> extends KafkaProducerMock<K,V> implements KafkaProducerTx<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public void sendAllTransactionally(String topic, Collection<KafkaMessage<K, V>> kafkaMessages) {
        logger.debug("Mocking Transactional Kafka Send! topic: {}, messages: {}", topic, kafkaMessages.size());
    }
}
