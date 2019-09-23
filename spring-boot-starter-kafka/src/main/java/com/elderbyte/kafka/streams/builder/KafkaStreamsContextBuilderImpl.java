package com.elderbyte.kafka.streams.builder;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.kafka.streams.builder.dsl.ElStreamsBuilder;
import com.elderbyte.kafka.streams.builder.dsl.KStreamSerde;
import com.elderbyte.kafka.streams.managed.StreamsCleanupConfig;
import com.elderbyte.kafka.streams.serdes.ElderJsonSerde;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContextImpl;
import com.elderbyte.kafka.streams.serdes.ElderKeySerde;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;


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
    private final StreamsBuilder streamsBuilder;
    private final ObjectMapper mapper;

    private boolean cleanUpOnStart = false;
    private boolean cleanUpOnStop = false;
    private boolean cleanUpOnError = false;

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
            KafkaStreamsConfiguration streamsConfig
    ) {
        if(streamsConfig == null) throw new ArgumentNullException("streamsConfig");
        this.mapper = mapper;
        this.streamsConfig = streamsConfig;
        this.streamsBuilder = new StreamsBuilder();
    }

    /***************************************************************************
     *                                                                         *
     * Config API                                                              *
     *                                                                         *
     **************************************************************************/

    public KafkaStreamsContextBuilder cleanUpOnStart(boolean cleanUp){
        this.cleanUpOnStart = cleanUp;
        return this;
    }

    public KafkaStreamsContextBuilder cleanUpOnStop(boolean cleanUp){
        this.cleanUpOnStop = cleanUp;
        return this;
    }

    public KafkaStreamsContextBuilder cleanUpOnError(boolean cleanUp){
        this.cleanUpOnError = cleanUp;
        return this;
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
    public <K,V> ElStreamsBuilder<K,V> from(Class<K> keyClazz, TypeReference<V> valueClazz){
        return from(
                new KStreamSerde<>(
                        ElderKeySerde.from(keyClazz),
                        ElderJsonSerde.from(mapper, valueClazz))
        );
    }

    @Override
    public <K,V> ElStreamsBuilder<K,V> from(Class<K> keyClazz, Class<V> valueClazz){
        return from(
                new KStreamSerde<>(
                        ElderKeySerde.from(keyClazz),
                        ElderJsonSerde.from(mapper, valueClazz))
        );
    }
    @Override
    public <K,V> ElStreamsBuilder<K,V> from(KStreamSerde<K,V> serde){
        return ElStreamsBuilder.from(this, serde);
    }

    /***************************************************************************
     *                                                                         *
     * Serdes                                                                  *
     *                                                                         *
     **************************************************************************/

    @Override
    public <K> Serde<K> keySerde(Class<K> keyClazz) {
        return ElderKeySerde.from(keyClazz);
    }

    @Override
    public <V> Serde<V> valueSerde(Class<V> valueClazz) {
        return ElderJsonSerde.from(mapper, valueClazz);
    }

    @Override
    public <V> Serde<V> valueSerde(TypeReference<V> valueClazz) {
        return ElderJsonSerde.from(mapper, valueClazz);
    }

    @Override
    public <V> KStreamSerde<String,V> serde(TypeReference<V> valueClazz){
        return serde(
                Serdes.String(),
                ElderJsonSerde.from(mapper, valueClazz)
        );
    }

    @Override
    public <V> KStreamSerde<String,V> serde(Class<V> valueClazz){
        return serde(
                Serdes.String(),
                ElderJsonSerde.from(mapper, valueClazz)
        );
    }

    @Override
    public <V> KStreamSerde<String,V> serde(Serde<V> valueSerde){
        return serde(
                Serdes.String(),
                valueSerde
        );
    }

    @Override
    public <K,V> KStreamSerde<K,V> serde(Class<K> keyClazz, TypeReference<V> valueClazz){
        return serde(
                ElderKeySerde.from(keyClazz),
                ElderJsonSerde.from(mapper, valueClazz)
        );
    }

    @Override
    public <K,V> KStreamSerde<K,V> serde(Class<K> keyClazz, Class<V> valueClazz){
        return serde(
                ElderKeySerde.from(keyClazz),
                ElderJsonSerde.from(mapper, valueClazz)
        );
    }

    @Override
    public <K,V> KStreamSerde<K,V> serde(Serde<K> keySerde, Serde<V> valueSerde) {
        return new KStreamSerde<>(
                keySerde,
                valueSerde
        );
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
                new StreamsCleanupConfig(cleanUpOnStart, cleanUpOnStop, cleanUpOnError),
                streams -> { }
        );
    }
}
