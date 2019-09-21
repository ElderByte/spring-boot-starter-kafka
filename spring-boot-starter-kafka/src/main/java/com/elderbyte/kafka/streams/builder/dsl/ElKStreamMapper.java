package com.elderbyte.kafka.streams.builder.dsl;

import com.elderbyte.kafka.streams.support.Transformers;
import com.elderbyte.kafka.streams.support.WithHeaderMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;

public class ElKStreamMapper<K,V, KR, VR> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final ElKStream<K,V> stream;
    private final ElStreamsBuilder<KR, VR> targetBuilder;
    private final KStreamSerde<KR, VR> targetSerde;

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
        this.targetSerde = targetSerde;
        this.targetBuilder = stream.builder().with(targetSerde);
    }

    /***************************************************************************
     *                                                                         *
     * Map KStream to KStream                                                  *
     *                                                                         *
     **************************************************************************/

    public ElKStream<KR, V> selectKey(KeyValueMapper<? super K, ? super V, ? extends KR> mapper){
        var myBuilder = builder().withKey(targetSerde.key());
        return myBuilder.el(stream.kstream().selectKey(mapper));
    }

    public ElKGroupedStream<KR,V> groupByKey(KeyValueMapper<? super K, ? super V, ? extends KR> mapper){
        return selectKey(mapper).groupByKey();
    }

    public ElKStream<KR, VR> map(KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper){
        return targetBuilder.el(stream.kstream().map(mapper));
    }

    public ElKStream<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper){
        var myBuilder = builder().withValue(targetSerde.value());
        return myBuilder.el(stream.kstream().mapValues(mapper));
    }

    public ElKStream<KR, VR> transform(WithHeaderMapper<K, V, KeyValue<KR, VR>> mapper){
        return transform(() -> Transformers.transformerWithHeader(mapper));
    }

    public ElKStream<K, VR> transformValues(WithHeaderMapper<K, V, VR> mapper){
        return transformValues(() -> Transformers.valueTransformerWithHeader(mapper));
    }

    public ElKStream<KR, VR> transform(TransformerSupplier<? super K, ? super V, KeyValue<KR, VR>> transformerSupplier,
                                       String... stateStoreNames
    ){
        var transformed = stream.kstream()
                .transform(transformerSupplier, stateStoreNames);
        return targetBuilder.el(transformed);
    }

    public ElKStream<K, VR> transformValues(ValueTransformerWithKeySupplier<? super K, ? super V, VR> valueTransformerSupplier,
                                            String... stateStoreNames
    ){
        var myBuilder = builder().withValue(targetSerde.value());

        var transformed = stream.kstream()
                .transformValues(valueTransformerSupplier, stateStoreNames);
        return myBuilder.el(transformed);
    }

    public ElKStream<KR, VR> flatMap(KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper){
        return targetBuilder.el(stream.kstream().flatMap(mapper));
    }

    public ElKStream<K, VR> flatMap(ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper){
        var myBuilder = builder().withValue(targetSerde.value());
        return myBuilder.el(stream.kstream().flatMapValues(mapper));
    }


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

    private ElStreamsBuilder<K,V> builder(){
        return stream.builder();
    }

}
