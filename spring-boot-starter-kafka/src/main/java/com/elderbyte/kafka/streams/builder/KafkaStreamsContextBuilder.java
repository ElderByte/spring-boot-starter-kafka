package com.elderbyte.kafka.streams.builder;

import com.elderbyte.kafka.streams.builder.cdc.CdcRecipesBuilder;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;


public interface KafkaStreamsContextBuilder {

    /***************************************************************************
     *                                                                         *
     * Builder API                                                             *
     *                                                                         *
     **************************************************************************/

    <V> KStream<String, V> streamOfJson(String topic, Class<V> clazz);
    <V> KStream<String, V> streamOfJson(String topic, TypeReference<V> clazz);

    /*
    <V> KTable<String, V> table(
            String storeName,
            KStream<String, Optional<V>> stream,
            Serde<V> valueSerde
    );

    <V> KTable<String, V> tableJson(
            String storeName,
            final KStream<String, Optional<V>> stream,
            Class<V> valueClazz
    );

    <V> KTable<String, V> tableJson(
            String storeName,
            final KStream<String, Optional<V>> stream,
            TypeReference<V> valueClazz
    );*/

    <CDCEvent, Entity> KTable<String, Entity> streamAsTable(
            String storeName,
            KStream<String, CDCEvent> cdcEventStream,
            KeyValueMapper<String, CDCEvent, KeyValue<String,Entity>> kvm,
            Class<Entity> clazz
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


}
