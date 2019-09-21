package com.elderbyte.kafka.streams.builder.dsl;

import com.elderbyte.kafka.streams.support.Transformers;
import com.elderbyte.kafka.streams.support.WithHeaderMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;

public class ElKTableMapper<K,V, KR, VR> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final ElKTable<K,V> table;
    private final ElStreamsBuilder<KR, VR> targetBuilder;
    private final KStreamSerde<KR, VR> targetSerde;

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
        this.targetSerde = targetSerde;
        this.targetBuilder = table.builder().with(targetSerde);
    }

    /***************************************************************************
     *                                                                         *
     * Map                                                                     *
     *                                                                         *
     **************************************************************************/

    public ElKTable<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, ElMat matConfig){
        var myBuilder = builder().withValue(targetSerde.value());
        return myBuilder.el(
                table.ktable()
                        .mapValues(
                                mapper,
                                myBuilder.serde().materialized(matConfig)
                        )
        );
    }

    public ElKTable<K, VR> transformValues(WithHeaderMapper<K, V, VR> mapper){
        return transformValues(() -> Transformers.valueTransformerWithHeader(mapper));
    }

    public ElKTable<K, VR> transformValues(ValueTransformerWithKeySupplier<? super K, ? super V, VR> valueTransformerSupplier,
                                            String... stateStoreNames
    ){
        var myBuilder = builder().withValue(targetSerde.value());

        var transformed = table.ktable()
                .transformValues(valueTransformerSupplier, stateStoreNames);
        return myBuilder.el(transformed);
    }

    /***************************************************************************
     *                                                                         *
     * Group Map                                                               *
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

    public ElKGroupedTable<KR, V> groupByKey(
            KeyValueMapper<K, V, KR> selector
    ){
        var keyBuilder = builder().withKey(targetBuilder.serde().key());
        var grp = table.ktable().groupBy(
                (k, v) -> KeyValue.pair(selector.apply(k, v), v),
                keyBuilder.serde().grouped()
        );
        return new ElKGroupedTable<>(keyBuilder, grp);
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
