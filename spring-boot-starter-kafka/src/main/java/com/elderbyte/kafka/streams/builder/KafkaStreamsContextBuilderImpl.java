package com.elderbyte.kafka.streams.builder;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.kafka.messages.MessageBlueprintFactory;
import com.elderbyte.kafka.streams.builder.dsl.ElStreamsBuilder;
import com.elderbyte.kafka.streams.builder.dsl.KStreamSerde;
import com.elderbyte.kafka.streams.builder.json.TombstoneJsonWrapper;
import com.elderbyte.kafka.streams.serdes.ElderJsonSerde;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContextImpl;
import com.elderbyte.kafka.streams.serdes.ElderKeySerde;
import com.elderbyte.kafka.streams.support.Transformers;
import com.elderbyte.messaging.api.ElderMessage;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.CleanupConfig;

import java.nio.charset.StandardCharsets;

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


    @Override
    public <V, MK, U extends ElderMessage<MK>, D extends ElderMessage<MK>> KTable<MK, U> mapStreamToMessagesTable(
            String storeName,
            KStream<String, V> inputStream,
            KeyValueMapper<String, V, UpdateOrDelete<MK, U, D>> kvm,
            Class<MK> keyClazz,
            Class<U> updateClazz
    ){
        var events = inputStream
                .transform(() -> Transformers.transformerWithHeader(
                        (k, v, headers) -> {

                            var messageHolder = kvm.apply(k, v);

                            var message = messageHolder.getMessage();

                            var messageSupport = MessageBlueprintFactory.lookupOrCreate(
                                    (Class<ElderMessage<MK>>)message.getClass()
                            );

                            var messageKey = messageSupport.getKey(message);

                            if(headers != null){
                                var messageHeaders = messageSupport.getHeaders(message);

                                messageHeaders.forEach((hKey, hVal) -> {
                                    headers.add(hKey, hVal.getBytes(StandardCharsets.UTF_8));
                                });
                            }

                            return KeyValue.pair(
                                    messageKey,
                                    TombstoneJsonWrapper.from(mapper, messageHolder)
                            );
                        }
                        )
                );

        return tableJson(
                storeName,
                events,
                keyClazz,
                updateClazz
        );
    }

    private  <K, V> KTable<K, V> tableJson(
            String storeName,
            KStream<K, TombstoneJsonWrapper<V>> stream,
            Class<K> keyClazz,
            Class<V> valueClazz
    ) {
        return stream
                .groupByKey(

                        serde(keyClazz, new TypeReference<TombstoneJsonWrapper<V>>() {}).grouped()
                )
                .aggregate( // Aggregate not reduce, since we change the Value type
                        () -> null,
                        (key, value, agg) -> value.getValue(mapper,valueClazz).orElse(null),
                        serde(keyClazz, valueClazz).materialized(storeName)
                            // .withLoggingDisabled()
                            // .withCachingDisabled()
                );
    }

    /***************************************************************************
     *                                                                         *
     * Serdes                                                                  *
     *                                                                         *
     **************************************************************************/

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
                cleanupConfig,
                streams -> { }
        );
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private  <V> KStream<String, V> streamFromJsonTopic(String topic, ElderJsonSerde<V> serde) {
        return streamsBuilder().stream(
                topic,
                serde(serde).consumed()
        );
    }
}
