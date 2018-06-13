package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.consumer.configuration.AutoOffsetReset;
import com.elderbyte.kafka.consumer.processing.Processor;
import com.elderbyte.kafka.metrics.MetricsContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;

/**
 * Provides the ability to build a kafka listener configuration
 */
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

    KafkaListenerBuilder<K,V> ignoreErrors();

    KafkaListenerBuilder<K,V> blockingRetries(int retries);


    /***************************************************************************
     *                                                                         *
     * Builder end API                                                         *
     *                                                                         *
     **************************************************************************/

    void startProcess(Processor<ConsumerRecord<K, V>> processor);

    void startProcessBatch(Processor<List<ConsumerRecord<K, V>>> processor);
}
