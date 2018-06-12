package com.elderbyte.kafka.consumer.factory;

import org.springframework.kafka.support.TopicPartitionInitialOffset;

import java.util.regex.Pattern;

/**
 * Provides the ability to build / configure kafka listeners
 */
public interface KafkaListenerFactory {

    KafkaListenerBuilder start(String... topics);

    KafkaListenerBuilder start(Pattern topicPattern);

    KafkaListenerBuilder start(TopicPartitionInitialOffset... topicPartitions);

}
