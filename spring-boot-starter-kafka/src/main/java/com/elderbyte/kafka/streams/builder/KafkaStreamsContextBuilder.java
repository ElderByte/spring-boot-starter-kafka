package com.elderbyte.kafka.streams.builder;

import com.elderbyte.kafka.streams.builder.cdc.CdcRecipesBuilder;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;


public interface KafkaStreamsContextBuilder {

    /***************************************************************************
     *                                                                         *
     * Builder API                                                             *
     *                                                                         *
     **************************************************************************/

    <V> KStream<String, V> streamOfJson(String topic, Class<V> clazz);
    <V> KStream<String, V> streamOfJson(String topic, TypeReference<V> clazz);


    StreamsBuilder streamsBuilder();

    default CdcRecipesBuilder cdcRecipes(){
        return new CdcRecipesBuilder(this);
    }

    /***************************************************************************
     *                                                                         *
     * Builder Finalize                                                        *
     *                                                                         *
     **************************************************************************/

    KafkaStreamsContext build();

    /***************************************************************************
     *                                                                         *
     * Support API                                                             *
     *                                                                         *
     **************************************************************************/

    <K,V> Serialized<K, V> serializedJson(Serde<K> keySerde, Class<V> valueClazz);

    <V> Serialized<String, V> serializedJson(Class<V> valueClazz);

     <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, TypeReference<V> clazz);

    <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Class<V> valueClazz);

    <K,V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Serde<K> keySerde, Class<V> valueClazz);


}
