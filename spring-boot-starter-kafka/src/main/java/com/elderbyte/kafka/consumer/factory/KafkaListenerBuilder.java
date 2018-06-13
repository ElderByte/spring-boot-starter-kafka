package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.consumer.processing.Processor;
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

     <NV> KafkaListenerBuilder<K,NV> valueDeserializer(Deserializer<NV> valueDeserializer);

     <NK> KafkaListenerBuilder<NK,V> keyDeserializer(Deserializer<NK> keyDeserializer);


    /***************************************************************************
     *                                                                         *
     * Builder end API                                                         *
     *                                                                         *
     **************************************************************************/

    void startProcess(Processor<ConsumerRecord<K, V>> processor);

    void startProcessBatch(Processor<List<ConsumerRecord<K, V>>> processor);
}
