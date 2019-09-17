package com.elderbyte.kafka.streams.support;

import org.apache.kafka.common.header.Headers;
import org.springframework.lang.Nullable;

@FunctionalInterface
public interface WithHeaderMapper<K, V, VR> {

    /**
     * Map the given value to a new value.
     *
     * @param value the value to be mapped
     * @param headers access / write headers of this record
     * @return the new value
     */
    VR apply(final K key, final V value, @Nullable Headers headers);
}
