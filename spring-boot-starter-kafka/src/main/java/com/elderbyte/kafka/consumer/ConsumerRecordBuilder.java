package com.elderbyte.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class ConsumerRecordBuilder {

    public static <K, V, M> ConsumerRecord<K, M> fromRecordWithValue(ConsumerRecord<K, V> record, M newValue){
        return fromRecordWithKeyValue(record, record.key(), newValue);
    }

    public static <K, V, M> ConsumerRecord<M, V> fromRecordWithKey(ConsumerRecord<K, V> record, M newKey){
        return fromRecordWithKeyValue(record, newKey, record.value());
    }

    public static <K,V> ConsumerRecord<K,V> fromRecordWithKeyValue(ConsumerRecord<?, ?> record, K key, V value){
        return new ConsumerRecord<>(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.timestampType(),
                null,
                record.serializedKeySize(),
                record.serializedValueSize(),
                key,
                value,
                record.headers()
        );
    }
}
