package com.elderbyte.kafka.streams.builder;

import com.elderbyte.kafka.streams.ElderJsonSerde;
import com.elderbyte.kafka.streams.builder.cdc.CdcRecipesBuilder;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;


public interface KafkaStreamsContextBuilder {

    /***************************************************************************
     *                                                                         *
     * Builder API                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a KStream from a json topic.
     */
    <V> KStream<String, V> streamOfJson(String topic, Class<V> clazz);

    /**
     * Creates a KStream from a json topic.
     */
    <V> KStream<String, V> streamOfJson(String topic, TypeReference<V> clazz);

    /**
     * Maps the given KStream values to a KTable.
     */
    <V, U, D> KTable<String, U> mapStreamToMessagesTable(
            String storeName,
            KStream<String, V> inputStream,
            KeyValueMapper<String, V, UpdateOrDelete<U, D>> kvm,
            Class<U> clazz
    );

    /**
     * Maps the given KStream values to a KTable.
     */
    <V, VR> KTable<String, VR> mapStreamToTable(
            String storeName,
            KStream<String, V> cdcEventStream,
            KeyValueMapper<String, V, KeyValue<String,VR>> kvm,
            Class<VR> clazz
    );

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

    <V> Produced<String, V> producedJson(Class<V> valueClazz);

    <K, V> Produced<K, V> producedJson(Serde<K> keySerde, Class<V> valueClazz);

}
