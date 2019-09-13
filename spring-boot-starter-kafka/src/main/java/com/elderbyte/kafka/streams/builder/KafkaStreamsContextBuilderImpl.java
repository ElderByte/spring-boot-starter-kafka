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
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.CleanupConfig;

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
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public StreamsBuilder streamsBuilder() {
        return this.streamsBuilder;
    }

    /***************************************************************************
     *                                                                         *
     * Support                                                                 *
     *                                                                         *
     **************************************************************************/

    public <K,V> Serialized<K, V> serializedJson(Serde<K> keySerde, Class<V> valueClazz){
        return Serialized.with(keySerde, ElderJsonSerde.from(mapper, valueClazz));
    }

    public <V> Serialized<String, V> serializedJson(Class<V> valueClazz){
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
        return Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(keySerde)
                .withValueSerde(ElderJsonSerde.from(mapper, valueClazz));
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
}
