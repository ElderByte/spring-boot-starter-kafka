package com.elderbyte.kafka.streams.support;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

public class ValueTransformerAdapter<K, V, VR> implements ValueTransformerWithKey<K, V, VR> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final ValueContextMapper<K, V, VR> contextMapper;

    private ProcessorContext context;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new HeaderWritterTransformer
     */
    ValueTransformerAdapter(ValueContextMapper<K, V, VR> contextMapper) {
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
    public VR transform(K readonlyKey, V value) {
        return contextMapper.apply(readonlyKey, value, context);
    }

    @Override
    public void close() { }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
