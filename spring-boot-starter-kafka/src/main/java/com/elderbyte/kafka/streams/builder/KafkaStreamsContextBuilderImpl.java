package com.elderbyte.kafka.streams.builder;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.kafka.messages.MessageBlueprint;
import com.elderbyte.kafka.messages.MessageBlueprintFactory;
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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
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
    public <V> KStream<String, V> streamFromJsonTopic(String topic, Class<V> clazz) {
        return streamFromJsonTopic(topic,  ElderJsonSerde.from(mapper, clazz));
    }

    @Override
    public <V> KStream<String, V> streamFromJsonTopic(String topic, TypeReference<V> clazz) {
        return streamFromJsonTopic(topic,  ElderJsonSerde.from(mapper, clazz));
    }

    @Override
    public <V> KTable<String, V> tableFromJsonTopic(String topic, Class<V> clazz, String storeName) {
        return tableFromJsonTopic(topic,  ElderJsonSerde.from(mapper, clazz), storeName);
    }

    @Override
    public <V> KTable<String, V> tableFromJsonTopic(String topic, TypeReference<V> clazz, String storeName) {
        return tableFromJsonTopic(topic,  ElderJsonSerde.from(mapper, clazz), storeName);
    }

    @Override
    public <V> GlobalKTable<String, V> globalTableFromJsonTopic(String topic, Class<V> clazz, String storeName) {
        return globalTableFromJsonTopic(topic,  ElderJsonSerde.from(mapper, clazz), storeName);
    }

    @Override
    public <V> GlobalKTable<String, V> globalTableFromJsonTopic(String topic, TypeReference<V> clazz, String storeName) {
        return globalTableFromJsonTopic(topic,  ElderJsonSerde.from(mapper, clazz), storeName);
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

                                MessageBlueprint<MK, ElderMessage<MK>> messageSupport = MessageBlueprintFactory.lookupOrCreate(message.getClass());

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

        var keySerde = ElderKeySerde.from(keyClazz);

        return stream
                .groupByKey(
                        groupedJson(keySerde, new TypeReference<TombstoneJsonWrapper<V>>() {})
                )
                .aggregate(
                    () -> null,
                    (k, value, oldValue) -> value.getValue(mapper,valueClazz).orElse(null),
                    materialized(storeName, keySerde, ElderJsonSerde.from(mapper, valueClazz))
                );
    }

    /**
     * Interprets the given KSTREAM as KTABLE, supporting key/value transformation.
     * Null values are translated into tombstone events.
     */
    @Override
    public <K, V, VR> KTable<K, VR> mapStreamToTable(
            String storeName,
            KStream<K, V> inputStream,
            KeyValueMapper<K, V, KeyValue<K,VR>> kvm,
            Class<K> keyClazz,
            Class<VR> valueClazz
    ){
        var events = inputStream
                .map(kvm)
                .mapValues((v) -> TombstoneJsonWrapper.ofNullable(mapper, v));

        return tableJson(
                storeName,
                events,
                keyClazz,
                valueClazz
        );
    }


    /***************************************************************************
     *                                                                         *
     * Support                                                                 *
     *                                                                         *
     **************************************************************************/

    public <K,V> Grouped<K, V> groupedJson(Serde<K> keySerde, Class<V> valueClazz){
        return Grouped.with(keySerde, ElderJsonSerde.from(mapper, valueClazz));
    }

    public <K,V> Grouped<K, V> groupedJson(Serde<K> keySerde, TypeReference<V> valueClazz){
        return Grouped.with(keySerde, ElderJsonSerde.from(mapper, valueClazz));
    }

    public <V> Grouped<String, V> groupedJson(Class<V> valueClazz){
        return groupedJson(Serdes.String(), valueClazz);
    }

    public <V> Grouped<String, V> groupedJson(TypeReference<V> valueClazz){
        return groupedJson(Serdes.String(), valueClazz);
    }

    public <V, VO> Joined<String, V, VO> joinedJson(
            Class<V> valueClazz,
            Class<VO> otherValueClazz
    ){
        return joinedJson(Serdes.String(), valueClazz, otherValueClazz);
    }

    public <K,V, VO> Joined<K, V, VO> joinedJson(
            Serde<K> keySerde,
            Class<V> valueClazz,
            Class<VO> otherValueClazz
    ){
        return Joined.with(
                keySerde,
                ElderJsonSerde.from(mapper, valueClazz),
                ElderJsonSerde.from(mapper, otherValueClazz)
        );
    }

    public<V, VO> Joined<String, V, VO> joinedJson(
            Class<V> valueClazz,
            TypeReference<VO> otherValueClazz
    ) {
        return joinedJson(Serdes.String(), valueClazz, otherValueClazz);
    }

    public <K,V, VO> Joined<K, V, VO> joinedJson(
            Serde<K> keySerde,
            Class<V> valueClazz,
            TypeReference<VO> otherValueClazz
    ) {
        return Joined.with(
                keySerde,
                ElderJsonSerde.from(mapper, valueClazz),
                ElderJsonSerde.from(mapper, otherValueClazz)
        );
    }


    public  <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, ElderJsonSerde<V> serde){
        return Materialized.<String, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(serde);
    }

    public  <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, TypeReference<V> clazz){
        return materializedJson(storeName, ElderJsonSerde.from(mapper, clazz));
    }

    public <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Class<V> valueClazz){
        return materializedJson(storeName, Serdes.String(), valueClazz);
    }

    public <K,V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Serde<K> keySerde, Class<V> valueClazz){
        return materialized(storeName, keySerde, ElderJsonSerde.from(mapper, valueClazz));
    }

    public  <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Serde<K> keySerde, TypeReference<V> clazz){
        return materialized(storeName, keySerde, ElderJsonSerde.from(mapper, clazz));
    }


    public <V> Produced<String, V> producedJson(Class<V> valueClazz) {
        return Produced.with(Serdes.String(), ElderJsonSerde.from(mapper, valueClazz));
    }

    public <K, V> Produced<K, V> producedJson(Serde<K> keySerde, Class<V> valueClazz) {
        return Produced.with(keySerde, ElderJsonSerde.from(mapper, valueClazz));
    }

    public <V> Produced<String, V> producedJson(TypeReference<V> valueClazz) {
        return Produced.with(Serdes.String(), ElderJsonSerde.from(mapper, valueClazz));
    }

    public <K, V> Produced<K, V> producedJson(Serde<K> keySerde, TypeReference<V> valueClazz) {
        return Produced.with(keySerde, ElderJsonSerde.from(mapper, valueClazz));
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

    private  <V> KStream<String, V> streamFromJsonTopic(String topic, ElderJsonSerde<V> serde) {
        return streamsBuilder().stream(
                topic,
                Consumed.with(
                        Serdes.String(),
                        serde
                )
        );
    }

    private  <V> KTable<String, V> tableFromJsonTopic(String topic, ElderJsonSerde<V> serde, String storeName) {
        return streamsBuilder().table(
                topic,
                materializedJson(storeName, serde)
                        .withLoggingDisabled()
        );
    }

    private  <V> GlobalKTable<String, V> globalTableFromJsonTopic(String topic, ElderJsonSerde<V> valueSerde, String storeName) {
        return streamsBuilder().globalTable(
                topic,
                materializedJson(storeName, valueSerde)
                        .withLoggingDisabled()
        );
    }
}
