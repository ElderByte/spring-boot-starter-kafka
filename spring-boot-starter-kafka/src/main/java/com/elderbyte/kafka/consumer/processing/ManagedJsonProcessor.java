package com.elderbyte.kafka.consumer.processing;

import com.elderbyte.kafka.consumer.ConsumerRecordBuilder;
import com.elderbyte.kafka.metrics.MetricsContext;
import com.elderbyte.kafka.metrics.MetricsReporter;
import com.elderbyte.kafka.serialisation.Json;
import com.elderbyte.kafka.serialisation.JsonMappingException;
import com.elderbyte.kafka.serialisation.JsonParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


import static java.util.stream.Collectors.toList;

/**
 * Manages the stream processing of json based data streams. Including metrics and error handling / reporting.
 * @param <K> The key type
 * @param <V> The value type (converted from json)
 */
public class ManagedJsonProcessor<K, V> {


    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final Logger log = LoggerFactory.getLogger(ManagedJsonProcessor.class);
    private final MetricsReporter reporter;
    private final Class<V> valueClazz;
    private final boolean skipOnError;
    private final boolean skipOnDtoMappingError;

    private final MetricsContext metricsCtx;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/


    public ManagedJsonProcessor(
            Class<V> valueClazz,
            boolean skipOnError,
            boolean skipOnDtoMappingError,
            MetricsReporter reporter,
            MetricsContext metricsContext){

        if(valueClazz == null) throw new IllegalArgumentException("valueClazz must not be null");
        if(reporter == null) throw new IllegalArgumentException("reporter must not be null");

        this.reporter = reporter;
        this.valueClazz = valueClazz;
        this.skipOnError = skipOnError;
        this.skipOnDtoMappingError = skipOnDtoMappingError;
        this.metricsCtx = metricsContext;
    }


    /***************************************************************************
     *                                                                         *
     * Public Api                                                              *
     *                                                                         *
     **************************************************************************/

    public boolean convertAndProcess(
            ConsumerRecord<K, Json> rawRecord,
            Processor<ConsumerRecord<K, V>> processor) throws UnrecoverableProcessingException {
        return convertAndProcess(rawRecord, processor, null);
    }

    public boolean convertAndProcess(
            ConsumerRecord<K, Json> rawRecord,
            Processor<ConsumerRecord<K, V>> processor,
            Acknowledgment ack) throws UnrecoverableProcessingException {

        return convertAndProcessAll(
                Collections.singletonList(rawRecord),
                records -> processor.proccess(records.get(0)),
                ack
                );
    }

    public boolean convertAndProcessAll(
            Collection<ConsumerRecord<K, Json>> rawRecords,
            Processor<List<ConsumerRecord<K, V>>> processor){
        return convertAndProcessAll(rawRecords, processor, null);
    }

    public boolean convertAndProcessAll(
            Collection<ConsumerRecord<K, Json>> rawRecords,
            Processor<List<ConsumerRecord<K, V>>> processor,
            Acknowledgment ack) throws UnrecoverableProcessingException {

        long start = System.nanoTime();

        var records = decodeAllRecords(rawRecords);

        // If we are here we have converted the records. Now run the user processing code.

        boolean success;
        if(skipOnError){
            success = processAllSkipOnError(records, processor, ack);
        }else{
            processAllErrorLoop(records, processor, ack);
            success = true;
        }

        if(success){
            reporter.reportStreamingMetrics(metricsCtx, records.size(), System.nanoTime() - start);
        }

        return success;
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/



    private boolean processAllSkipOnError(
            List<ConsumerRecord<K, V>> records,
            Processor<List<ConsumerRecord<K, V>>> processor,
            Acknowledgment ack){

        boolean success;
        try {
            processor.proccess(records);
            success = true;
        }catch (Exception e){
            reporter.reportProcessingError(metricsCtx, records, e);
            success = false;
        }finally {
            if(ack != null) { ack.acknowledge(); }
        }
        return success;
    }


    private void processAllErrorLoop(
            List<ConsumerRecord<K, V>> records,
            Processor<List<ConsumerRecord<K, V>>> processor,
            Acknowledgment ack){

        if(records == null) throw new IllegalArgumentException("records must not be null");
        if(processor == null) throw new IllegalArgumentException("processor must not be null");
        if(ack == null) throw new IllegalArgumentException("ack must not be null");

        int errorLoopIteration = 0;
        boolean errorLoop;

        do{
            try{
                processor.proccess(records);
                ack.acknowledge();
                errorLoop = false;
            }catch (Exception e){
                errorLoop = true;
                ++errorLoopIteration;
                log.warn("Error while processing records! Retry " + errorLoopIteration, e);

                reporter.reportProcessingError(metricsCtx, records, e, errorLoopIteration);

                try {
                    // Error Backoff
                    Thread.sleep(Math.min(errorLoopIteration * 1000, 1000*60));
                } catch (InterruptedException e1) { }
            }

        } while (errorLoop);
    }

    private List<ConsumerRecord<K, V>> decodeAllRecords(Collection<ConsumerRecord<K, Json>> rawRecords) throws UnrecoverableProcessingException {
        List<ConsumerRecord<K, V>> records;

        try {
            records = rawRecords.stream()
                    .map(r -> decodeRecord(r, skipOnError || skipOnDtoMappingError))
                    .filter(Objects::nonNull) // Skipped records will be null
                    .collect(toList());
        }catch (Exception e){

            // In case a json record failed to map to our dto and don't skip those, its over.

            reporter.reportUnrecoverableCrash(metricsCtx, rawRecords, e);
            throw new UnrecoverableProcessingException("Failed to parse/convert record", e);
        }
        return records;
    }

    /**
     * Decodes the payload of a json record into a java DTO.
     *
     * In case skipOnDtoMappingError is false and a record is malformed, it will throw an exception.
     *
     * @param record The raw json param
     * @param skipOnError In case there is a mapping error, skip and return null.
     * @return Returns the mapped record. If errors are skipped, might return null if mapping has failed.
     */
    private ConsumerRecord<K, V> decodeRecord(ConsumerRecord<K, Json> record, boolean skipOnError){
        if(record.value() != null){
            try{
                V value = record.value().json(valueClazz);
                return ConsumerRecordBuilder.fromRecordWithValue(record, value);
            }catch (JsonParseException e){
                // Not even valid json! Assume poison message.
                reporter.reportMalformedRecord(metricsCtx, record, e); // Report
                return null; // Skip
            }catch (JsonMappingException e) {

                reporter.reportMalformedRecord(metricsCtx, record, e);

                if(skipOnError){
                    return null; // Skip
                }else{
                    throw e; // Escalate
                }
            }
        }else{
            return ConsumerRecordBuilder.fromRecordWithValue(record, null);
        }
    }




}
