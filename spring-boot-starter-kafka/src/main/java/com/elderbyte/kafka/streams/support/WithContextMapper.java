package com.elderbyte.kafka.streams.support;

import org.apache.kafka.streams.processor.ProcessorContext;

public interface WithContextMapper<K, V, VR> {

    /**
     * Map the given value to a new value.
     *
     * @param value the value to be mapped
     * @param context access the current context of this value record
     * @return the new value
     */
    VR apply(final K key, final V value, final ProcessorContext context);
}

