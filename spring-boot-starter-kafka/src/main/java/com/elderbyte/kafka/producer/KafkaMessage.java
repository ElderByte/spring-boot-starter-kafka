package com.elderbyte.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaMessage<K,V> {

    /***************************************************************************
     *                                                                         *
     * Static builders                                                         *
     *                                                                         *
     **************************************************************************/


    public static <K,V> KafkaMessage<K,V> tombstone(K key){

        if(key == null) throw new IllegalArgumentException("key must not be null!");

        return new KafkaMessage<>(key, null, null, null);
    }

    public static <K,V> KafkaMessage<K,V> build(K key, V value){

        if(key == null) throw new IllegalArgumentException("key must not be null!");
        if(value == null) throw new IllegalArgumentException("value must not be null");

        return new KafkaMessage<>(key, value, null, null);
    }

    public static <K,V> KafkaMessage<K,V> build(K key, V value, int partition){

        if(key == null) throw new IllegalArgumentException("key must not be null!");
        if(value == null) throw new IllegalArgumentException("value must not be null");

        return new KafkaMessage<>(key, value, partition, null);
    }

    public static <K,V> KafkaMessage<K,V> build(K key, V value, long timestamp){

        if(key == null) throw new IllegalArgumentException("key must not be null!");
        if(value == null) throw new IllegalArgumentException("value must not be null");

        return new KafkaMessage<>(key, value, null, timestamp);
    }

    public static <K,V> KafkaMessage<K,V> build(K key, V value, int partition, long timestamp){

        if(key == null) throw new IllegalArgumentException("key must not be null!");
        if(value == null) throw new IllegalArgumentException("value must not be null");

        return new KafkaMessage<>(key, value, partition, timestamp);
    }


    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final K key;
    private final V value;
    private final Integer partition;
    private final Long timestamp;

    // TODO Support Headers

    /***************************************************************************
     *                                                                         *
     * Constructors                                                            *
     *                                                                         *
     **************************************************************************/

    private KafkaMessage(K key, V value, Integer partition, Long timestamp) {
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.timestamp = timestamp;
    }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public Integer getPartition() {
        return partition;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "key=" + key +
                ", value=" + value +
                ", partition=" + partition +
                ", timestamp=" + timestamp +
                '}';
    }

    /**
     * Convert this message to a producer record
     * @param topic The topic for the record.
     */
    public ProducerRecord<K,V> toRecord(String topic){
        return new ProducerRecord<>(
                topic,
                this.getPartition(),
                this.getTimestamp(),
                this.getKey(),
                this.getValue()
        );
    }
}