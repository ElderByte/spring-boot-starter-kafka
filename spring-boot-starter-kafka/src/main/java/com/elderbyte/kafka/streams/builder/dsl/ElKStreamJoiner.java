package com.elderbyte.kafka.streams.builder.dsl;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.*;

public class ElKStreamJoiner<K,V, VR> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final ElKStream<K, V> stream;
    private final KStreamSerde<K, VR> targetSerde;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElKTableJoiner
     */
    public ElKStreamJoiner(
            ElKStream<K, V> stream,
            KStreamSerde<K, VR> targetSerde
    ) {
        this.stream = stream;
        this.targetSerde = targetSerde;
    }

    /***************************************************************************
     *                                                                         *
     * JOIN Stream To Table                                                    *
     *                                                                         *
     **************************************************************************/

    public <VO> ElKStream<K, VR> leftJoin(
            final ElKTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner
    ){
        return leftJoin(other, joiner, null);
    }

    public <VO> ElKStream<K, VR> leftJoin(
            final ElKTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner,
            Serde<VO> otherValueSerde
    ){
        KStream<K, VR> joined;

        if(otherValueSerde == null){
            joined = kstream()
                    .leftJoin(
                            other.ktable(),
                            joiner
                    );
        }else{
            joined = kstream()
                    .leftJoin(
                            other.ktable(),
                            joiner,
                            builder().serde().joined(otherValueSerde)
                    );
        }
        return builder().with(targetSerde).el(joined);
    }


    public <VO> ElKStream<K, VR> join(
            final ElKTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner
    ){
        return join(other, joiner, null);
    }

    public <VO> ElKStream<K, VR> join(
            final ElKTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner,
            Serde<VO> otherValueSerde
    ){
        KStream<K, VR> joined;

        if(otherValueSerde == null){
            joined = kstream()
                    .join(
                            other.ktable(),
                            joiner
                    );
        }else{
            joined = kstream()
                    .join(
                            other.ktable(),
                            joiner,
                            builder().serde().joined(otherValueSerde)
                    );
        }
        return builder().with(targetSerde).el(joined);
    }

    /***************************************************************************
     *                                                                         *
     * JOIN Stream To Stream                                                   *
     *                                                                         *
     **************************************************************************/

    public <VO> ElKStream<K, VR> join(
            final ElKStream<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner,
            JoinWindows windows
    ){
        return leftJoin(other, joiner, windows);
    }

    public <VO> ElKStream<K, VR> join(
            final ElKStream<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner,
            JoinWindows windows,
            Serde<VO> otherValueSerde
    ){
        KStream<K, VR> joined;

        if(otherValueSerde == null){
            joined = kstream()
                    .join(
                            other.kstream(),
                            joiner,
                            windows
                    );
        }else{
            joined = kstream()
                    .join(
                            other.kstream(),
                            joiner,
                            windows,
                            builder().serde().joined(otherValueSerde)
                    );
        }
        return builder().with(targetSerde).el(joined);
    }


    public <VO> ElKStream<K, VR> leftJoin(
            final ElKStream<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner,
            JoinWindows windows
    ){
        return leftJoin(other, joiner, windows);
    }

    public <VO> ElKStream<K, VR> leftJoin(
            final ElKStream<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner,
            JoinWindows windows,
            Serde<VO> otherValueSerde
    ){
        KStream<K, VR> joined;

        if(otherValueSerde == null){
            joined = kstream()
                    .leftJoin(
                            other.kstream(),
                            joiner,
                            windows
                    );
        }else{
            joined = kstream()
                    .leftJoin(
                            other.kstream(),
                            joiner,
                            windows,
                            builder().serde().joined(otherValueSerde)
                    );
        }
        return builder().with(targetSerde).el(joined);
    }

    public <VO> ElKStream<K, VR> outerJoin(
            final ElKStream<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner,
            JoinWindows windows
    ){
        return outerJoin(other, joiner, windows, null);
    }

    public <VO> ElKStream<K, VR> outerJoin(
            final ElKStream<K, VO> other,
            final ValueJoiner<? super V, ? super VO, VR> joiner,
            JoinWindows windows,
            Serde<VO> otherValueSerde
    ){
        KStream<K, VR> joined;

        if(otherValueSerde == null){
            joined = kstream()
                    .outerJoin(
                            other.kstream(),
                            joiner,
                            windows
                    );
        }else{
            joined = kstream()
                    .outerJoin(
                            other.kstream(),
                            joiner,
                            windows,
                            builder().serde().joined(otherValueSerde)
                    );
        }
        return builder().with(targetSerde).el(joined);
    }

    /***************************************************************************
     *                                                                         *
     * JOIN Stream To KGlobalTable                                             *
     *                                                                         *
     **************************************************************************/

    public <GK, GV> ElKStream<K, VR> join(
            final ElGlobalKTable<GK, GV> other,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keyValueMapper,
            final ValueJoiner<? super V, ? super GV, VR> joiner
    ){
        KStream<K, VR> joined;

        joined = kstream()
                .join(
                        other.table(),
                        keyValueMapper,
                        joiner
                );

        return builder().with(targetSerde).el(joined);
    }

    public <GK, GV> ElKStream<K, VR> leftJoin(
            final ElGlobalKTable<GK, GV> other,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keyValueMapper,
            final ValueJoiner<? super V, ? super GV, VR> joiner
    ){
        KStream<K, VR> joined;

        joined = kstream()
                .leftJoin(
                        other.table(),
                        keyValueMapper,
                        joiner
                );

        return builder().with(targetSerde).el(joined);
    }


    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private KStream<K,V> kstream(){
        return stream.kstream();
    }

    private ElStreamsBuilder<K,V> builder(){
        return stream.builder();
    }

}
