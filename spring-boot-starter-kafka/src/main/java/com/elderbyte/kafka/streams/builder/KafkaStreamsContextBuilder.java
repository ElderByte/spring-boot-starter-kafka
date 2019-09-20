package com.elderbyte.kafka.streams.builder;

import com.elderbyte.kafka.streams.builder.dsl.ElStreamsBuilder;
import com.elderbyte.kafka.streams.builder.dsl.KStreamSerde;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.elderbyte.messaging.api.ElderMessage;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;


public interface KafkaStreamsContextBuilder {

    /***************************************************************************
     *                                                                         *
     * Builder API                                                             *
     *                                                                         *
     **************************************************************************/

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

    <V> KStreamSerde<String,V> serde(TypeReference<V> valueClazz);

    <V> KStreamSerde<String,V> serde(Class<V> valueClazz);

    <V> KStreamSerde<String,V> serde(Serde<V> valueSerde);

    <K,V> KStreamSerde<K,V> serde(Class<K> keyClazz, TypeReference<V> valueClazz);

    <K,V> KStreamSerde<K,V> serde(Class<K> keyClazz, Class<V> valueClazz);

    <K,V> KStreamSerde<K,V> serde(Serde<K> keySerde, Serde<V> valueSerde);
}
