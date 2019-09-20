package com.elderbyte.kafka.streams.builder.dsl;

import com.elderbyte.kafka.streams.support.Transformers;
import com.elderbyte.kafka.streams.support.WithHeaderMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

public class ElKStreamMapper<K,V, KR, VR> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final ElKStream<K,V> stream;
    private final ElStreamsBuilder<KR, VR> targetBuilder;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElKStreamMapper
     */
    public ElKStreamMapper(
            ElKStream<K,V> stream,
            KStreamSerde<KR, VR> targetSerde
    ) {
        this.stream = stream;
        this.targetBuilder = stream.builder().with(targetSerde);
    }

    /***************************************************************************
     *                                                                         *
     * Map KStream to KStream                                                  *
     *                                                                         *
     **************************************************************************/

    public ElKStream<KR, V> selectKey(KeyValueMapper<? super K, ? super V, ? extends KR> mapper){
        var myBuilder = stream.builder().with(stream.serde().withKey(targetBuilder.serde()));
        return myBuilder.el(stream.kstream().selectKey(mapper));
    }

    public ElKStream<KR, VR> map(KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper){
        return targetBuilder.el(stream.kstream().map(mapper));
    }

    public ElKStream<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper){
        var myBuilder = stream.builder().with(stream.serde().withValue(targetBuilder.serde()));
        return myBuilder.el(stream.kstream().mapValues(mapper));
    }

    public ElKStream<KR, VR> transform(WithHeaderMapper<K, V, KeyValue<KR, VR>> mapper){
        var transformed = stream.kstream()
                .transform(() -> Transformers.transformerWithHeader(mapper));
        return targetBuilder.el(transformed);
    }

    // TODO flat*Map

    /***************************************************************************
     *                                                                         *
     * Map KStream to KTable                                                   *
     *                                                                         *
     **************************************************************************/


    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/


}
