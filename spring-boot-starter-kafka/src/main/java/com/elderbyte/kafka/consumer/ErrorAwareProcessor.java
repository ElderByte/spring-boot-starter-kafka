package com.elderbyte.kafka.consumer;

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

public class ErrorAwareProcessor<K, V> {


    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final Logger log = LoggerFactory.getLogger(ErrorAwareProcessor.class);
    private final MetricsReporter reporter;
    private final Class<V> valueClazz;
    private final boolean skipOnError;
    private final boolean skipOnDtoMappingError;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/


    public ErrorAwareProcessor(Class<V> valueClazz, boolean skipOnError, boolean skipOnDtoMappingError, MetricsReporter reporter){
        if(valueClazz == null) throw new IllegalArgumentException("valueClazz must not be null");
        if(reporter == null) throw new IllegalArgumentException("reporter must not be null");
        this.reporter = reporter;
        this.valueClazz = valueClazz;
        this.skipOnError = skipOnError;
        this.skipOnDtoMappingError = skipOnDtoMappingError;
    }


    /***************************************************************************
     *                                                                         *
     * Public Api                                                              *
     *                                                                         *
     **************************************************************************/

    public boolean convertAndProcess(
            ConsumerRecord<K, Json> rawRecord,
            Processor<ConsumerRecord<K, V>> processor,
            Acknowledgment ack){

        return convertAndProcessAll(
                Collections.singletonList(rawRecord),
                records -> processor.proccess(records.get(0)),
                ack
                );
    }

    public boolean convertAndProcessAll(
            Collection<ConsumerRecord<K, Json>> rawRecords,
            Processor<List<ConsumerRecord<K, V>>> processor,
            Acknowledgment ack){

        long start = System.nanoTime();

        List<ConsumerRecord<K, V>> records;

        try {
            records = rawRecords.stream()
                    .map(this::convertRecord)
                    .filter(Objects::nonNull) // Skipped records will be null
                    .collect(toList());
        }catch (Exception e){

            if(skipOnError){
                return false; // We cant even try to process, just skip this
            }else{
                reporter.reportUnrecoverableCrash(rawRecords, e);
                throw new UnrecoverableProcessingException("Failed to parse/convert record", e);
            }
        }

        // If we are here we have converted the records. Now run the user processing code.

        boolean success;
        if(skipOnError){
            success = processAllSkipOnError(records, processor, ack);
        }else{
            processAllErrorLoop(records, processor, ack);
            success = true;
        }

        reporter.reportStreamingMetrics(records.size(), (System.nanoTime() - start) /  (1000 * 1000));

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
            reporter.reportProcessingError(records, e);
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

                reporter.reportProcessingError(records, e, errorLoopIteration);

                try {
                    // Error Backoff
                    Thread.sleep(Math.min(errorLoopIteration * 1000, 1000*60));
                } catch (InterruptedException e1) { }
            }

        } while (errorLoop);
    }

    private ConsumerRecord<K, V> convertRecord(ConsumerRecord<K, Json> record){
        if(record.value() != null){
            try{
                V value = record.value().json(valueClazz);
                return ConsumerRecordBuilder.fromRecordWithValue(record, value);
            }catch (JsonParseException e){
                // Not even valid json! Assume poison message.
                reporter.reportMalformedRecord(record, e); // Report
                return null; // Skip
            }catch (JsonMappingException e) {

                reporter.reportMalformedRecord(record, e);

                if(skipOnDtoMappingError){
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
