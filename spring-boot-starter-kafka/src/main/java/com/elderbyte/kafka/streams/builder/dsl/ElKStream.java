package com.elderbyte.kafka.streams.builder.dsl;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class ElKStream<K,V> extends ElStreamBase<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KStream<K,V> kstream;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElKtable
     */
    public ElKStream(KStream<K,V> kstream, ElStreamsBuilder<K,V> elBuilder) {
        super(elBuilder);
        this.kstream = kstream;
    }

    /***************************************************************************
     *                                                                         *
     * Grouping API                                                            *
     *                                                                         *
     **************************************************************************/

    public ElKGroupedStream<K,V> groupByKey(){
        return new ElKGroupedStream<>(
                builder(),
                kstream.groupByKey(serde().grouped())
        );
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Get the underlying KStream
     */
    public KStream<K, V> kstream() {
        return kstream;
    }

    public ElKStream<K,V> peek(final ForeachAction<? super K, ? super V> action){
        kstream.peek(action);
        return this;
    }

    public void foreach(final ForeachAction<? super K, ? super V> action){
        kstream.foreach(action);
    }

    public ElKStream<K,V> filter(final Predicate<? super K, ? super V> predicate){
        return builder().el(kstream.filter(predicate));
    }

    public ElKStream<K,V> merge(ElKStream<K,V> otherStream){
        return builder().el(kstream.merge(otherStream.kstream()));
    }

    public void to(TopicNameExtractor<K, V> topicExtractor){
        kstream.to(topicExtractor, serde().produced());
    }

    public void to(String topicName){
        kstream.to(topicName, serde().produced());
    }

    public ElKStream<K,V> through(String topicName){
        return builder().el(
                kstream.through(topicName, serde().produced())
        );
    }

    public List<ElKStream<K, V>> branch(final Predicate<? super K, ? super V>... predicates){
        var branches = kstream.branch(predicates);

        return Arrays.stream(branches)
                .map(b -> builder().el(b))
                .collect(toList());
    }

    /***************************************************************************
     *                                                                         *
     * MAP  API                                                                *
     *                                                                         *
     **************************************************************************/

    public <KR,VR> ElKStreamMapper<K,V, KR, VR> mapTo(Class<KR> newKey, Class<VR> newValue){
        return mapTo(context().serde(newKey, newValue));
    }

    public <KR,VR> ElKStreamMapper<K,V, KR, VR> mapTo(KStreamSerde<KR, VR> newSerde){
        return new ElKStreamMapper<>(this, newSerde);
    }

    public <VR> ElKStreamMapper<K,V, K, VR> mapToValue(Serde<VR> newValue){
        return new ElKStreamMapper<>(this, serde().withValue(newValue));
    }

    public <VR> ElKStreamMapper<K,V, K, VR> mapToValue(Class<VR> newValue){
        return mapToValue(context().serde(newValue).value());
    }

    public <KR> ElKStreamMapper<K,V, KR, V> mapToKey(Serde<KR> newKey){
        return new ElKStreamMapper<>(this, serde().withKey(newKey));
    }

    public <KR> ElKStreamMapper<K,V, KR, V> mapToKey(Class<KR> newKey){
        return mapToKey(context().keySerde(newKey));
    }

    /***************************************************************************
     *                                                                         *
     * Join API                                                                *
     *                                                                         *
     **************************************************************************/

    public ElKStreamJoiner<K, V, V> joiner(){
        return new ElKStreamJoiner<>(this, serde());
    }

    public <VR> ElKStreamJoiner<K, V, VR> joiner(Serde<VR> newValueSerde){
        return new ElKStreamJoiner<>(this, serde().withValue(newValueSerde));
    }

    public <VR> ElKStreamJoiner<K, V, VR> joiner(KStreamSerde<K, VR> newSerde){
        return new ElKStreamJoiner<>(this, newSerde);
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
