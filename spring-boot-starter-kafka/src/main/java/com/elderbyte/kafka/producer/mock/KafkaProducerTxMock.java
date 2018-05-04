package com.elderbyte.kafka.producer.mock;

import com.elderbyte.kafka.producer.KafkaMessage;
import com.elderbyte.kafka.producer.KafkaProducerTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
    public List<CompletableFuture<SendResult<K, V>>> sendAllTransactionally(String topic, Collection<KafkaMessage<K, V>> kafkaMessages) {
        logger.debug("Mocking Transactional Kafka Send! topic: {}, messages: {}", topic, kafkaMessages.size());
        return new ArrayList<>();
    }
}
