package com.elderbyte.kafka.consumer.factory;

import org.springframework.kafka.support.TopicPartitionInitialOffset;

import java.util.regex.Pattern;

/**
 * Provides the ability to build / configure kafka listeners
 */
public interface KafkaListenerFactory {

    KafkaListenerBuilder<byte[], byte[]> start(String... topics);

    KafkaListenerBuilder<byte[], byte[]> start(Pattern topicPattern);

    KafkaListenerBuilder<byte[], byte[]> start(TopicPartitionInitialOffset... topicPartitions);

}
