package com.elderbyte.kafka.streams.builder.dsl;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.*;

public class ElKGroupedTable<K,V> extends ElStreamBase<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KGroupedTable<K,V> groupedTable;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElKGroupedStream
     */
    public ElKGroupedTable(ElStreamsBuilder<K,V> elBuilder, KGroupedTable<K,V> groupedTable) {
        super(elBuilder);
        this.groupedTable = groupedTable;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    public KGroupedTable<K,V> groupedTable(){
        return groupedTable;
    }

    public ElKTable<K,V> reduce(Reducer<V> adder, Reducer<V> subtractor, String storeName){
        return builder()
                .el(
                        groupedTable()
                                .reduce(
                                        adder,
                                        subtractor,
                                        serde().materialized(storeName)
                                )
                );
    }

    public ElKTable<K,V> aggregate(
            final Initializer<V> initializer,
            final Aggregator<K, V, V> adder,
            final Aggregator<K, V, V> subtractor,
            String storeName
    ){
        return builder()
                .el(
                        groupedTable()
                                .aggregate(
                                        initializer,
                                        adder,
                                        subtractor,
                                        serde().materialized(storeName)
                                )
                );
    }

    public <VR> ElKTable<K,VR> aggregateMap(
            final Initializer<VR> initializer,
            final Aggregator<K, V, VR> adder,
            final Aggregator<K, V, VR> subtractor,
            String storeName,
            TypeReference<VR> clazz
    ){
        return aggregateMap(
                initializer,
                adder,
                subtractor,
                storeName,
                context().serde(clazz).value()
        );
    }

    public <VR> ElKTable<K,VR> aggregateMap(
            final Initializer<VR> initializer,
            final Aggregator<K, V, VR> adder,
            final Aggregator<K, V, VR> subtractor,
            String storeName,
            Class<VR> clazz
    ){
        return aggregateMap(
                initializer,
                adder,
                subtractor,
                storeName,
                context().serde(clazz).value()
        );
    }

    public <VR> ElKTable<K,VR> aggregateMap(
            final Initializer<VR> initializer,
            final Aggregator<K, V, VR> adder,
            final Aggregator<K, V, VR> subtractor,
            String storeName,
            Serde<VR> serde
    ){

        var newSerde = serde().withValue(serde);

        return builder().with(newSerde).el(
                groupedTable()
                        .aggregate(
                                initializer,
                                adder,
                                subtractor,
                                newSerde.materialized(storeName)
                        )
        );
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
