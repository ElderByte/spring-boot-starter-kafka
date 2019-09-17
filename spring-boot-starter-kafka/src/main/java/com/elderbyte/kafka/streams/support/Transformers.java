package com.elderbyte.kafka.streams.support;

import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

public class Transformers<K, V, VR> implements Transformer<K, V, VR> {


    public static <K, V,VR> ValueTransformerWithKey<K, V, VR> valueTransformerWithContext(WithContextMapper<K, V, VR> mapper){
        return new WithContextMapperTransformer<>(mapper);
    }

    public static <K, V,VR> Transformer<K, V, VR> transformerWithContext(WithContextMapper<K, V, VR> mapper){
        return new Transformers<>(mapper);
    }

    public static <K, V,VR> ValueTransformerWithKey<K, V, VR> valueTransformerWithHeader(WithHeaderMapper<K, V, VR> mapper){
        return valueTransformerWithContext(
                        (k, v, ctx) -> mapper.apply(k, v, ctx.headers())
                );
    }

    public static <K, V,VR> Transformer<K, V, VR> transformerWithHeader(WithHeaderMapper<K, V, VR> mapper){
        return transformerWithContext(
                        (k, v, ctx) -> mapper.apply(k, v, ctx.headers())
                );
    }

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final WithContextMapper<K, V, VR> contextMapper;

    private ProcessorContext context;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new HeaderWritterTransformer
     */
    Transformers(WithContextMapper<K, V, VR> contextMapper) {
        this.contextMapper = contextMapper;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public VR transform(K key, V value) {
        return contextMapper.apply(key, value, context);
    }

    @Override
    public void close() { }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
