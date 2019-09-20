package com.elderbyte.kafka.streams.builder;

import com.elderbyte.kafka.streams.builder.cdc.CdcRecipesBuilder;
import com.elderbyte.kafka.streams.builder.dsl.ElStreamsBuilder;
import com.elderbyte.kafka.streams.builder.dsl.KStreamSerde;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.elderbyte.messaging.api.ElderMessage;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;


public interface KafkaStreamsContextBuilder {

    /***************************************************************************
     *                                                                         *
     * Builder API                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a KStream from a json topic.
     */
    @Deprecated
    <V> KStream<String, V> streamFromJsonTopic(String topic, TypeReference<V> clazz);

    /**
     * Maps the given KStream values to a KTable.
     */
    @Deprecated
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
    @Deprecated
    <K, V, VR> KTable<K, VR> mapStreamToTable(
            String storeName,
            KStream<K, V> inputStream,
            KeyValueMapper<K, V, KeyValue<K,VR>> kvm,
            Class<K> keyClazz,
            Class<VR> valueClazz
    );


    /***************************************************************************
     *                                                                         *
     * Builder API                                                             *
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
     * Serdes                                                                  *
     *                                                                         *
     **************************************************************************/

    <V> KStreamSerde<String,V> serde(TypeReference<V> valueClazz);

    <V> KStreamSerde<String,V> serde(Class<V> valueClazz);

    <V> KStreamSerde<String,V> serde(Serde<V> valueSerde);

    <K,V> KStreamSerde<K,V> serde(Class<K> keyClazz, TypeReference<V> valueClazz);

    <K,V> KStreamSerde<K,V> serde(Class<K> keyClazz, Class<V> valueClazz);

    <K,V> KStreamSerde<K,V> serde(Serde<K> keySerde, Serde<V> valueSerde);
}
