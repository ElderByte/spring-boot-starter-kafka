package com.elderbyte.kafka.streams.builder;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.kafka.streams.ElderJsonSerde;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContextImpl;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.CleanupConfig;

import java.util.Optional;

/**
 *
 *
 * Note: Based on StreamsBuilderFactoryBean
 */
public class KafkaStreamsContextBuilderImpl implements KafkaStreamsContextBuilder {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsContextBuilderImpl.class);

    private final KafkaStreamsConfiguration streamsConfig;
    private final CleanupConfig cleanupConfig;
    private final StreamsBuilder streamsBuilder;
    private final ObjectMapper mapper;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new KafkaStreamsBuilderImpl
     */
    public KafkaStreamsContextBuilderImpl(
            ObjectMapper mapper,
            KafkaStreamsConfiguration streamsConfig,
            CleanupConfig cleanupConfig
    ) {
        if(streamsConfig == null) throw new ArgumentNullException("streamsConfig");
        if(cleanupConfig == null) throw new ArgumentNullException("cleanupConfig");

        this.mapper = mapper;
        this.streamsConfig = streamsConfig;
        this.cleanupConfig = cleanupConfig;
        this.streamsBuilder = new StreamsBuilder();
    }


    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public StreamsBuilder streamsBuilder() {
        return this.streamsBuilder;
    }

    @Override
    public <V> KStream<String, V> streamOfJson(String topic, Class<V> clazz) {
        return streamOfJson(topic,  ElderJsonSerde.from(mapper, clazz));
    }

    @Override
    public <V> KStream<String, V> streamOfJson(String topic, TypeReference<V> clazz) {
        return streamOfJson(topic,  ElderJsonSerde.from(mapper, clazz));
    }

    /*
    @Override
    public <V> KTable<String, V> tableJson(
            String storeName,
            final KStream<String, Optional<V>> stream,
            Class<V> valueClazz
    ) {
        return table(
                storeName,
                stream,
                ElderJsonSerde.from(mapper, valueClazz)
        );
    }

    @Override
    public <V> KTable<String, V> tableJson(
            String storeName,
            final KStream<String, Optional<V>> stream,
            TypeReference<V> valueClazz
    ) {
        return table(
                storeName,
                stream,
                ElderJsonSerde.from(mapper, valueClazz)
        );
    }*/

    //@Override

    public <CDCEvent, Entity> KTable<String, Entity> streamAsTable(
            String storeName,
            KStream<String, CDCEvent> cdcEventStream,
            KeyValueMapper<String, CDCEvent, KeyValue<String,Entity>> kvm,
            Class<Entity> clazz
    ){

        var events = cdcEventStream
                .map(kvm::apply)
                .mapValues((v) -> TombstoneJsonWrapper.ofNullable(mapper, v));

        return tableJson(
                storeName,
                events,
                clazz
        );

          /*
        .groupByKey(builder.serializedJson(clazz))
        .reduce(
                (old, current) -> current, // TODO Handle deletes from current.deleted flag
                builder.materializedJson(storeName, clazz)
                        .withLoggingDisabled() // Good bad ?
        );*/
    }

    private  <V> KTable<String, V> tableJson(
            String storeName,
            KStream<String, TombstoneJsonWrapper<V>> stream,
            Class<V> valueClazz
    ) {
        return stream
                .groupByKey(
                        serializedJson(new TypeReference<TombstoneJsonWrapper<V>>() {})
                )
                .aggregate(
                    () -> null,
                    (k, value, oldValue) -> value.getValue(mapper,valueClazz).orElse(null),
                    materialized(storeName, Serdes.String(), ElderJsonSerde.from(mapper, valueClazz))
                );
    }


    /***************************************************************************
     *                                                                         *
     * Support                                                                 *
     *                                                                         *
     **************************************************************************/

    public <K,V> Serialized<K, V> serializedJson(Serde<K> keySerde, Class<V> valueClazz){
        return Serialized.with(keySerde, ElderJsonSerde.from(mapper, valueClazz));
    }

    public <K,V> Serialized<K, V> serializedJson(Serde<K> keySerde, TypeReference<V> valueClazz){
        return Serialized.with(keySerde, ElderJsonSerde.from(mapper, valueClazz));
    }

    public <V> Serialized<String, V> serializedJson(Class<V> valueClazz){
        return serializedJson(Serdes.String(), valueClazz);
    }

    public <V> Serialized<String, V> serializedJson(TypeReference<V> valueClazz){
        return serializedJson(Serdes.String(), valueClazz);
    }

    public  <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, TypeReference<V> clazz){
        return Materialized.<String, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(ElderJsonSerde.from(mapper, clazz));
    }

    public <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Class<V> valueClazz){
        return materializedJson(storeName, Serdes.String(), valueClazz);
    }

    public <K,V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Serde<K> keySerde, Class<V> valueClazz){
        return materialized(storeName, keySerde, ElderJsonSerde.from(mapper, valueClazz));
    }

    public <K,V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized(String storeName, Serde<K> keySerde, Serde<V> valueSerde){
        return Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }

    /***************************************************************************
     *                                                                         *
     * Finalize Build                                                          *
     *                                                                         *
     **************************************************************************/

    @Override
    public KafkaStreamsContext build() {

        var topology = this.streamsBuilder.build();

        return new KafkaStreamsContextImpl(
                topology,
                streamsConfig,
                cleanupConfig,
                streams -> { }
        );
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private  <V> KStream<String, V> streamOfJson(String topic, ElderJsonSerde<V> serde) {
        return streamsBuilder().stream(
                topic,
                Consumed.with(
                        Serdes.String(),
                        serde
                )
        );
    }
}
