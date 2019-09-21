package com.elderbyte.kafka.streams.builder.dsl;

import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class ElKTableJoiner<K,V, VR> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final ElKTable<K, V> table;
    private final KStreamSerde<K, VR> targetSerde;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElKTableJoiner
     */
    public ElKTableJoiner(
            ElKTable<K, V> table,
            KStreamSerde<K, VR> targetSerde
    ) {
        this.table = table;
        this.targetSerde = targetSerde;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    public <VO> ElKTable<K, VR> leftJoin(
            final ElKTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner
    ){
        return leftJoin(other, joiner, null);
    }

    public <VO> ElKTable<K, VR> leftJoin(
            final ElKTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner,
            ElMat matConfig
    ){
        KTable<K, VR> joined;

        if(matConfig == null){
            joined = ktable()
                    .leftJoin(
                            other.ktable(),
                            joiner
                    );
        }else{
            joined = ktable()
                    .leftJoin(
                            other.ktable(),
                            joiner,
                            targetSerde.materialized(matConfig)
                    );
        }
        return builder().with(targetSerde).el(joined);
    }

    public <VO> ElKTable<K, VR> join(
            final ElKTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner
    ){
        return join(other, joiner, null);
    }

    public <VO> ElKTable<K, VR> join(
            final ElKTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner,
            ElMat matConfig
    ){
        KTable<K, VR> joined;

        if(matConfig == null){
            joined = ktable()
                    .join(
                            other.ktable(),
                            joiner
                    );
        }else{
            joined = ktable()
                    .join(
                            other.ktable(),
                            joiner,
                            targetSerde.materialized(matConfig)
                    );
        }
        return builder().with(targetSerde).el(joined);
    }

    public <VO> ElKTable<K, VR> outerJoin(
            final ElKTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner
    ){
        return outerJoin(other, joiner, null);
    }

    public <VO> ElKTable<K, VR> outerJoin(
            final ElKTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner,
            ElMat matConfig
    ){
        KTable<K, VR> joined;

        if(matConfig == null){
            joined = ktable()
                    .outerJoin(
                            other.ktable(),
                            joiner
                    );
        }else{
            joined = ktable()
                    .outerJoin(
                            other.ktable(),
                            joiner,
                            targetSerde.materialized(matConfig)
                    );
        }
        return builder().with(targetSerde).el(joined);
    }


    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private KTable<K,V> ktable(){
        return table.ktable();
    }

    private ElStreamsBuilder<K,V> builder(){
        return table.builder();
    }

}
