package com.elderbyte.kafka.streams.builder.dsl;

import org.apache.kafka.streams.kstream.*;

import java.util.function.Function;

public class ElKGroupedStream<K,V> extends ElStreamBase<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KGroupedStream<K,V> groupedStream;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElKGroupedStream
     */
    public ElKGroupedStream(ElStreamsBuilder<K,V> elBuilder, KGroupedStream<K,V> groupedStream) {
        super(elBuilder);
        this.groupedStream = groupedStream;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    public KGroupedStream<K,V> groupedStream(){
        return groupedStream;
    }

    public ElKTable<K,V> reduce(Reducer<V> reducer, String storeName){
        return builder()
                .el(
                        groupedStream()
                                .reduce(
                                        reducer,
                                        serde().materialized(storeName)
                                )
                );
    }

    public ElKTable<K,V> aggregate(
            final Initializer<V> initializer,
            final Aggregator<? super K, ? super V, V> aggregator,
            String storeName
    ){
        return builder()
                .el(
                        groupedStream()
                                .aggregate(
                                        initializer,
                                        aggregator,
                                        serde().materialized(storeName)
                                )
                );
    }

    public <VR> ElKTable<K,V> aggregateNative(Function<KGroupedStream<K,V>, KTable<K, V>> nativeAggregate){
        return builder().el(nativeAggregate.apply(groupedStream));
    }

    /*
    public <VR> ElKTable<K,V> aggregateMap(
            final Initializer<VR> initializer,
            final Aggregator<? super K, ? super V, VR> aggregator,
            String storeName
    ){
        return builder()
                .el(
                        groupedStream()
                                .aggregate(
                                        initializer,
                                        aggregator,
                                        serde().materialized(storeName)
                                )
                );
    }*/

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
