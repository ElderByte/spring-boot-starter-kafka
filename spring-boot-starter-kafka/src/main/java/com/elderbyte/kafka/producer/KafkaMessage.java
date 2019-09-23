package com.elderbyte.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.springframework.lang.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class KafkaMessage<K,V> {

    /***************************************************************************
     *                                                                         *
     * Static builders                                                         *
     *                                                                         *
     **************************************************************************/

    /**
     * Build a kafka message from an annotated message object.
     */
    public static <K, M> KafkaMessage<K, M> fromMessage(M messageObject){
        return AnnotationKafkaMessageBuilder.build(messageObject);
    }

    /**
     * Builds a message which might be a value or tombstone.
     * @param key The message key. Must not be null.
     * @param value The message value which might be null. (tombstone)
     */
    public static <K,V> KafkaMessage<K,V> buildSave(K key, V value) {
        if(key == null) throw new IllegalArgumentException("key must not be null!");

        return value != null ? build(key, value) : tombstone(key);
    }

    public static <K,V> KafkaMessage<K,V> tombstone(K key){
        return tombstone(key, null);
    }

    public static <K,V> KafkaMessage<K,V> tombstone(K key, Map<String, String> headers){

        if(key == null) throw new IllegalArgumentException("key must not be null!");

        return new KafkaMessage<>(key, null, null, null, headers);
    }

    public static <K,V> KafkaMessage<K,V> build(K key, V value){
        return build(key, value, null);
    }

    public static <K,V> KafkaMessage<K,V> build(K key, V value, Map<String, String> headers){

        if(key == null) throw new IllegalArgumentException("key must not be null!");
        if(value == null) throw new IllegalArgumentException("value must not be null");

        return new KafkaMessage<>(key, value, null, null, headers);
    }

    public static <K,V> KafkaMessage<K,V> build(K key, V value, int partition){

        if(key == null) throw new IllegalArgumentException("key must not be null!");
        if(value == null) throw new IllegalArgumentException("value must not be null");

        return new KafkaMessage<>(key, value, partition, null, null);
    }

    public static <K,V> KafkaMessage<K,V> build(K key, V value, long timestamp){

        if(key == null) throw new IllegalArgumentException("key must not be null!");
        if(value == null) throw new IllegalArgumentException("value must not be null");

        return new KafkaMessage<>(key, value, null, timestamp, null);
    }

    public static <K,V> KafkaMessage<K,V> build(K key, V value, int partition, long timestamp){
        return build(key, value, partition, timestamp, null);
    }

    public static <K,V> KafkaMessage<K,V> build(K key, V value, int partition, long timestamp, Map<String, String> headers){

        if(key == null) throw new IllegalArgumentException("key must not be null!");
        if(value == null) throw new IllegalArgumentException("value must not be null");

        return new KafkaMessage<>(key, value, partition, timestamp, headers);
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
    private final Map<String, String> headers = new HashMap<>();

    /***************************************************************************
     *                                                                         *
     * Constructors                                                            *
     *                                                                         *
     **************************************************************************/

    private KafkaMessage(K key, V value, Integer partition, Long timestamp, @Nullable Map<String, String> headers) {
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.timestamp = timestamp;

        if(headers != null){
            this.headers.putAll(headers);
        }

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

    public Map<String, String> getHeaders() {
        return headers;
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
        return new ProducerRecord<K, V>(
                topic,
                this.getPartition(),
                this.getTimestamp(),
                this.getKey(),
                this.getValue(),
                recordHeaders()
        );
    }

    /**
     * Convert this message to a consumer record
     * @param topic
     * @param offset
     * @return
     */
    public ConsumerRecord<K,V> toConsumerRecord(String topic, long offset){
        return toConsumerRecord(topic, offset, null, 0, 0);
    }

    /**
     * Convert this message to a consumer record
     * @param topic The topic for the record.
     */
    public ConsumerRecord<K,V> toConsumerRecord(String topic, long offset, Long checksum, int serializedKeySize, int serializedValueSize){

        return new ConsumerRecord<>(
                topic,
                this.getPartition() != null ? this.getPartition() : 0,
                offset,
                this.getTimestamp() != null ? this.getTimestamp() : 0,
                TimestampType.CREATE_TIME,
                checksum,
                serializedKeySize,
                serializedValueSize,
                this.getKey(),
                this.getValue(),
                new RecordHeaders(recordHeaders().toArray(new Header[0]))
        );
    }


    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private List<Header> recordHeaders(){
        return headers.entrySet().stream()
                .map(es -> new RecordHeader(es.getKey(), es.getValue().getBytes(StandardCharsets.UTF_8)))
                .collect(toList());
    }
}
