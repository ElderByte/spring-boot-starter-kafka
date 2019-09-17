package com.elderbyte.kafka.streams.support;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.springframework.lang.Nullable;

@FunctionalInterface
public interface ValueHeaderMapper<K, V, VR> {

    static <K, V,VR> ValueTransformerWithKey<K, V, VR> valueTransformer(ValueHeaderMapper<K, V, VR> mapper){
        return ValueContextMapper
                .valueTransformer(
                        (k, v, ctx) -> mapper.apply(k, v, ctx.headers())
                );
    }

    /**
     * Map the given value to a new value.
     *
     * @param value the value to be mapped
     * @param headers access / write headers of this record
     * @return the new value
     */
    VR apply(final K key, final V value, @Nullable Headers headers);
}
