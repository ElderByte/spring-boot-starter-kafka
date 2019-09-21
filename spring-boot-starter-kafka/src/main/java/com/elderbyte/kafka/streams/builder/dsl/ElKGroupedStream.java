package com.elderbyte.kafka.streams.builder.dsl;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.*;

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

    public ElKTable<K,V> latest(
            String storeName
    ){
        return reduce((current, agg) -> current, storeName);
    }

    public <VR> ElKTable<K,VR> latest(
            final KeyValueMapper<K, V, VR> mapper,
            ElMat store,
            Class<VR> clazz
    ){
        // Aggregate not reduce, since we change the Value type
        return aggregateMap(
                () -> null,
                (k, current, agg) -> mapper.apply(k, current),
                store,
                context().serde(clazz).value()
        );
    }


    public <VR> ElKTable<K,VR> aggregateMap(
            final Initializer<VR> initializer,
            final Aggregator<K, V, VR> aggregator,
            ElMat store,
            Class<VR> clazz
    ){
        return aggregateMap(initializer, aggregator, store, context().serde(clazz).value());
    }

    public <VR> ElKTable<K,VR> aggregateMap(
            final Initializer<VR> initializer,
            final Aggregator<K, V, VR> aggregator,
            ElMat store,
            Serde<VR> serde
    ){

        var newSerde = serde().withValue(serde);

        return builder().with(newSerde).el(
                groupedStream()
                        .aggregate(
                                initializer,
                                aggregator,
                                newSerde.materialized(store)
                        )
        );
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
