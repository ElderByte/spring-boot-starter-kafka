package com.elderbyte.kafka.consumer.processing;

import com.elderbyte.kafka.consumer.ConsumerRecordBuilder;
import com.elderbyte.kafka.metrics.MetricsContext;
import com.elderbyte.kafka.metrics.MetricsReporter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class RecordBatchDecoder<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final MetricsReporter reporter;
    private final MetricsContext metricsCtx;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/


    public RecordBatchDecoder(
            MetricsReporter reporter,
            MetricsContext metricsCtx,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer
    ){
        if(reporter == null) throw new IllegalArgumentException("reporter");
        if(metricsCtx == null) throw new IllegalArgumentException("metricsCtx");
        if(keyDeserializer == null) throw new IllegalArgumentException("keyDeserializer");
        if(valueDeserializer == null) throw new IllegalArgumentException("valueDeserializer");

        this.reporter = reporter;
        this.metricsCtx = metricsCtx;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    /***************************************************************************
     *                                                                         *
     * Public Api                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Decodes all records using the configured deserializers
     */
    public List<ConsumerRecord<K, V>> decodeAllRecords(List<ConsumerRecord<byte[], byte[]>> rawRecords) throws UnrecoverableProcessingException {
        List<ConsumerRecord<K, V>> records;

        try {
            records = rawRecords.stream()
                    .map(this::decodeRecord)
                    .filter(Objects::nonNull) // Skipped records will be null
                    .collect(toList());
        }catch (Exception e){

            reporter.reportUnrecoverableCrash(metricsCtx, rawRecords, e);
            throw new UnrecoverableProcessingException("Failed to parse/convert record", e);
        }
        return records;
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private K deserializeKey(ConsumerRecord<byte[], byte[]> record){
        try {
            return keyDeserializer.deserialize(record.topic(), record.key());
        }catch (Exception e){
            throw new ProcessingException("Failed to deserialize record key!(" + record.serializedKeySize() + ")", e);
        }
    }

    private V deserializeValue(ConsumerRecord<byte[], byte[]> record){
        try {
            return valueDeserializer.deserialize(record.topic(), record.value());
        }catch (Exception e){
            throw new ProcessingException("Failed to deserialize record value! (" + record.serializedValueSize() + ")", e);
        }
    }

    /**
     * Decodes the payload of a json record into a java DTO.
     *
     * In case skipOnDtoMappingError is false and a record is malformed, it will throw an exception.
     *
     * @param record The raw json param
     * @return Returns the mapped record. If errors are skipped, might return null if mapping has failed.
     */
    private ConsumerRecord<K, V> decodeRecord(ConsumerRecord<byte[], byte[]> record){

        K decodedKey;
        // Decode key
        try {
            decodedKey = deserializeKey(record);
        }catch (Exception e){
            // Not able do deserialize key
            reporter.reportMalformedRecord(metricsCtx, record, e); // Report key explicitly
            return null; // Skip
        }

        V decodedValue;
        // Decode Value
        if(record.value() != null){
            try{
                decodedValue = deserializeValue(record);
                return ConsumerRecordBuilder.fromRecordWithKeyValue(record, decodedKey, decodedValue);
            }catch (Exception e){
                // Not able do deserialize value
                // Assume poison message
                reporter.reportMalformedRecord(metricsCtx, record, e); // Report
                return null; // Skip
            }
        }else{
            return ConsumerRecordBuilder.fromRecordWithKeyValue(record, decodedKey,null);
        }
    }
}
