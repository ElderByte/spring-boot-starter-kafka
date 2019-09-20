package com.elderbyte.kafka.streams.builder;

import com.elderbyte.kafka.streams.builder.cdc.CdcRecipesBuilder;
import com.elderbyte.kafka.streams.builder.dsl.ElStreamsBuilder;
import com.elderbyte.kafka.streams.builder.dsl.KStreamSerde;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.elderbyte.messaging.api.ElderMessage;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Serde;
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
    <V> KStream<String, V> streamFromJsonTopic(String topic, Class<V> clazz);

    /**
     * Creates a KStream from a json topic.
     */
    <V> KStream<String, V> streamFromJsonTopic(String topic, TypeReference<V> clazz);

    <V> KTable<String, V> tableFromJsonTopic(String topic, Class<V> clazz, String storeName);

    <V> KTable<String, V> tableFromJsonTopic(String topic, TypeReference<V> clazz, String storeName);


    <V> GlobalKTable<String, V> globalTableFromJsonTopic(String topic, Class<V> clazz, String storeName);

    <V> GlobalKTable<String, V> globalTableFromJsonTopic(String topic, TypeReference<V> clazz, String storeName);

    /**
     * Maps the given KStream values to a KTable.
     */
    <V, MK, U extends ElderMessage<MK>, D extends ElderMessage<MK>> KTable<MK, U> mapStreamToMessagesTable(
            String storeName,
            KStream<String, V> inputStream,
            KeyValueMapper<String, V, UpdateOrDelete<MK, U, D>> kvm,
            Class<MK> keyClazz,
            Class<U> updateClazz
    );

    /**
     * Maps the given KStream values to a KTable.
     */
    <K, V, VR> KTable<K, VR> mapStreamToTable(
            String storeName,
            KStream<K, V> inputStream,
            KeyValueMapper<K, V, KeyValue<K,VR>> kvm,
            Class<K> keyClazz,
            Class<VR> valueClazz
    );


    /***************************************************************************
     *                                                                         *
     * El Builder                                                              *
     *                                                                         *
     **************************************************************************/


    <K,V> ElStreamsBuilder<K,V> from(Class<K> keyClazz, TypeReference<V> valueClazz);

    <K,V> ElStreamsBuilder<K,V> from(Class<K> keyClazz, Class<V> valueClazz);

    <K,V> ElStreamsBuilder<K,V> from(KStreamSerde<K,V> serde);

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

    <K,V> Grouped<K, V> groupedJson(Serde<K> keySerde, Class<V> valueClazz);

    <V> Grouped<String, V> groupedJson(Class<V> valueClazz);

    <V, VO> Joined<String, V, VO> joinedJson(
            Class<V> valueClazz,
            Class<VO> otherValueClazz
    );

    <K,V, VO> Joined<K, V, VO> joinedJson(
            Serde<K> keySerde,
            Class<V> valueClazz,
            Class<VO> otherValueClazz
    );

    <V, VO> Joined<String, V, VO> joinedJson(
            Class<V> valueClazz,
            TypeReference<VO> otherValueClazz
    );

    <K,V, VO> Joined<K, V, VO> joinedJson(
            Serde<K> keySerde,
            Class<V> valueClazz,
            TypeReference<VO> otherValueClazz
    );

     <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, TypeReference<V> clazz);

     <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Serde<K> keySerde, TypeReference<V> clazz);

    <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Class<V> valueClazz);

    <K,V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Serde<K> keySerde, Class<V> valueClazz);

    <V> Produced<String, V> producedJson(Class<V> valueClazz);

    <K, V> Produced<K, V> producedJson(Serde<K> keySerde, Class<V> valueClazz);

    <V> Produced<String, V> producedJson(TypeReference<V> valueClazz);

    <K, V> Produced<K, V> producedJson(Serde<K> keySerde, TypeReference<V> valueClazz);

}
