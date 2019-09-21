package com.elderbyte.kafka.streams.builder.dsl;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class ElKTableMapper<K,V, KR, VR> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final ElKTable<K,V> table;
    private final ElStreamsBuilder<KR, VR> targetBuilder;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElKTableMapper
     */
    public ElKTableMapper(
            ElKTable<K, V> table,
            KStreamSerde<KR, VR> targetSerde) {
        this.table = table;
        this.targetBuilder = table.builder().with(targetSerde);
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    public ElKGroupedTable<KR, VR> groupBy(
            KeyValueMapper<K, V, KeyValue<KR,VR>> selector
    ){
        var grp = table.ktable().groupBy(
                selector,
                targetBuilder.serde().grouped()
        );
        return new ElKGroupedTable<>(targetBuilder, grp);
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private ElStreamsBuilder<K,V> builder(){
        return table.builder();
    }
}
