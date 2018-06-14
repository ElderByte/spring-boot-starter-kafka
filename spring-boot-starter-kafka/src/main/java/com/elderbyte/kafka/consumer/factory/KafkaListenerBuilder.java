package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.consumer.configuration.AutoOffsetReset;
import com.elderbyte.kafka.consumer.processing.Processor;
import com.elderbyte.kafka.metrics.MetricsContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import java.util.List;

/**
 * Provides the ability to build a kafka listener configuration
 */
@InterfaceStability.Evolving
public interface KafkaListenerBuilder<K,V> {

    /***************************************************************************
     *                                                                         *
     * Builder API                                                             *
     *                                                                         *
     **************************************************************************/

    <NV> KafkaListenerBuilder<K,NV> jsonValue(Class<NV> valueClazz);

    <NK> KafkaListenerBuilder<NK,V> jsonKey(Class<NK> keyClazz);

    KafkaListenerBuilder<K,String> stringValue();

    KafkaListenerBuilder<String,V> stringKey();

    <NV> KafkaListenerBuilder<K,NV> valueDeserializer(Deserializer<NV> valueDeserializer);

    <NK> KafkaListenerBuilder<NK,V> keyDeserializer(Deserializer<NK> keyDeserializer);

    KafkaListenerBuilder<K,V>  autoOffsetReset(AutoOffsetReset autoOffsetReset);

    KafkaListenerBuilder<K,V> consumerGroup(String groupId);

    KafkaListenerBuilder<K,V> consumerId(String clientId);

    KafkaListenerBuilder<K,V> metrics(MetricsContext metricsContext);

    /**
     * Skip records on error
     */
    KafkaListenerBuilder<K,V> ignoreErrors();

    /**
     * On error, retry the given number of times.
     * After that, a errorhandler is invoked and the records are skipped.
     */
    KafkaListenerBuilder<K,V> blockingRetries(int retries);

    /**
     * Enable / Disable auto-commit. Default is false to support error handling.
     */
    KafkaListenerBuilder<K, V> autoCommit(boolean autoCommit);


    /**
     * Callback for rebalance handler.
     *
     * @deprecated Will be removed once supported natively by the builder
     */
    @InterfaceStability.Unstable
    KafkaListenerBuilder<K, V> rebalanceListener(ConsumerAwareRebalanceListener rebalanceListener); // TODO custom builder

    /**
     * Set whether or not to call consumer.commitSync() or commitAsync() when the
     * container is responsible for commits. Default true.
     */
    KafkaListenerBuilder<K, V> syncCommits(boolean syncCommits);

    /**
     * Set the max time to block in the consumer waiting for records.
     */
    KafkaListenerBuilder<K, V> pollTimeout(long pollTimeout);


    /***************************************************************************
     *                                                                         *
     * Builder end API                                                         *
     *                                                                         *
     **************************************************************************/

    void startProcess(Processor<ConsumerRecord<K, V>> processor);

    void startProcessBatch(Processor<List<ConsumerRecord<K, V>>> processor);
}
