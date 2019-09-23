package com.elderbyte.kafka.streams.builder;

import com.elderbyte.kafka.streams.builder.dsl.ElStreamsBuilder;
import com.elderbyte.kafka.streams.builder.dsl.KStreamSerde;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;


public interface KafkaStreamsContextBuilder {

    KafkaStreamsContextBuilder cleanUpOnError(boolean cleanUp);

    KafkaStreamsContextBuilder cleanUpOnStart(boolean cleanUp);

    KafkaStreamsContextBuilder cleanUpOnStop(boolean cleanUp);

    /***************************************************************************
     *                                                                         *
     * Builder API                                                             *
     *                                                                         *
     **************************************************************************/


    <K,V> ElStreamsBuilder<K,V> from(Class<K> keyClazz, TypeReference<V> valueClazz);

    <K,V> ElStreamsBuilder<K,V> from(Class<K> keyClazz, Class<V> valueClazz);

    <K,V> ElStreamsBuilder<K,V> from(KStreamSerde<K,V> serde);

    StreamsBuilder streamsBuilder();


    /***************************************************************************
     *                                                                         *
     * Builder Finalize                                                        *
     *                                                                         *
     **************************************************************************/

    KafkaStreamsContext build();

    /***************************************************************************
     *                                                                         *
     * Serdes                                                                  *
     *                                                                         *
     **************************************************************************/

    <K> Serde<K> keySerde(Class<K> keyClazz);

    <V> Serde<V> valueSerde(Class<V> valueClazz);
    <V> Serde<V> valueSerde(TypeReference<V> valueClazz);

    <V> KStreamSerde<String,V> serde(TypeReference<V> valueClazz);

    <V> KStreamSerde<String,V> serde(Class<V> valueClazz);

    <V> KStreamSerde<String,V> serde(Serde<V> valueSerde);

    <K,V> KStreamSerde<K,V> serde(Class<K> keyClazz, TypeReference<V> valueClazz);

    <K,V> KStreamSerde<K,V> serde(Class<K> keyClazz, Class<V> valueClazz);

    <K,V> KStreamSerde<K,V> serde(Serde<K> keySerde, Serde<V> valueSerde);
}
