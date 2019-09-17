package com.elderbyte.kafka.streams.support;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

public interface ValueContextMapper<K, V, VR> {

    static <K, V,VR> ValueTransformerWithKey<K, V, VR> valueTransformer(ValueContextMapper<K, V, VR> mapper){
        return new ValueTransformerAdapter<>(mapper);
    }

    /**
     * Map the given value to a new value.
     *
     * @param value the value to be mapped
     * @param context access the current context of this value record
     * @return the new value
     */
    VR apply(final K key, final V value, final ProcessorContext context);
}

