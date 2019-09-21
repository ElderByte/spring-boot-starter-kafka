package com.elderbyte.kafka.streams.builder.dsl;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Suppressed;

import java.util.Optional;

public class ElKTable<K,V> extends ElStreamBase<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KTable<K,V> ktable;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElKtable
     */
    public ElKTable(KTable<K,V> ktable, ElStreamsBuilder<K,V> elBuilder) {
        super(elBuilder);
        this.ktable = ktable;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    public KTable<K, V> ktable() {
        return ktable;
    }

    public ElKTable<K,V> suppress(final Suppressed<? super K> suppressed){
        return builder().el(ktable().suppress(suppressed));
    }

    public ElKTable<K,V> filter(final Predicate<? super K, ? super V> predicate){
        return builder().el(ktable().filter(predicate));
    }

    public ElKStream<K,V> toStream(){
        return builder().el(ktable().toStream());
    }

    /***************************************************************************
     *                                                                         *
     * MAP  API                                                                *
     *                                                                         *
     **************************************************************************/

    public <KR,VR> ElKTableMapper<K,V, KR, VR> mapTo(Class<KR> newKey, Class<VR> newValue){
        return mapTo(context().serde(newKey, newValue));
    }

    public <KR,VR> ElKTableMapper<K,V, KR, VR> mapTo(KStreamSerde<KR, VR> newSerde){
        return new ElKTableMapper<>(this, newSerde);
    }

    public <VR> ElKTableMapper<K,V, K, VR> mapToValue(Serde<VR> newValue){
        return new ElKTableMapper<>(this, serde().withValue(newValue));
    }

    public <VR> ElKTableMapper<K,V, K, VR> mapToValue(TypeReference<VR> newValue){
        return mapToValue(context().serde(newValue).value());
    }

    public <VR> ElKTableMapper<K,V, K, VR> mapToValue(Class<VR> newValue){
        return mapToValue(context().serde(newValue).value());
    }

    public <KR> ElKTableMapper<K,V, KR, V> mapToKey(Serde<KR> newKey){
        return new ElKTableMapper<>(this, serde().withKey(newKey));
    }

    public <KR> ElKTableMapper<K,V, KR, V> mapToKey(Class<KR> newKey){
        return mapToKey(context().keySerde(newKey));
    }

    /***************************************************************************
     *                                                                         *
     * Joiner API                                                              *
     *                                                                         *
     **************************************************************************/

    public ElKTableJoiner<K, V, V> joiner(){
        return new ElKTableJoiner<>(this, serde());
    }

    public <VR> ElKTableJoiner<K, V, VR> joiner(Serde<VR> newValueSerde){
        return new ElKTableJoiner<>(this, serde().withValue(newValueSerde));
    }

    public <VR> ElKTableJoiner<K, V, VR> joiner(KStreamSerde<K, VR> newSerde){
        return new ElKTableJoiner<>(this, newSerde);
    }

    /***************************************************************************
     *                                                                         *
     * Store                                                                   *
     *                                                                         *
     **************************************************************************/

    public Optional<String> queryableStoreName(){
        return Optional.ofNullable(ktable.queryableStoreName());
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
