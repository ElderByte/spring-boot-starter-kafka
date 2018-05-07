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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.util.stream.Collectors.toList;

public class ErrorAwareProcessor {


    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final Logger log = LoggerFactory.getLogger(ErrorAwareProcessor.class);
    private final MetricsReporter reporter;

    public ErrorAwareProcessor(MetricsReporter reporter){
        this.reporter = reporter;
    }


    /***************************************************************************
     *                                                                         *
     * Public Api                                                              *
     *                                                                         *
     **************************************************************************/



    public <K, V> boolean convertAndProcessAll(
            Collection<ConsumerRecord<K, Json>> rawRecords,
            Class<V> clazz,
            Processor<List<ConsumerRecord<K, V>>> processor,
            Acknowledgment ack,
            boolean skipOnError){

        List<ConsumerRecord<K, V>> records;

        try {
            records = rawRecords.stream()
                    .map(r -> convertRecord(r, clazz))
                    .filter(Objects::nonNull) // Skipped records will be null
                    .collect(toList());
        }catch (Exception e){
            // todo report failed record
            throw new UnrecoverableProcessingException("Failed to parse/convert record", e);
        }

        // If we are here we have converted the records. Now run the user processing code.

        boolean success;
        if(skipOnError){
            success = processAllSkipOnError(records, processor, ack);
        }else{
            processAllErrorLoop(records, processor, ack);
            success = true;
        }
        return success;
    }

    public <K, V> boolean processAllSkipOnError(
            List<ConsumerRecord<K, V>> records,
            Processor<List<ConsumerRecord<K, V>>> processor,
            Acknowledgment ack){

        boolean success;
        try {
            processor.proccess(records);
            success = true;
        }catch (Exception e){
            // TODO Report
            success = false;
        }finally {
            if(ack != null) { ack.acknowledge(); }
        }
        return success;
    }


    public <K, V> void processAllErrorLoop(
            List<ConsumerRecord<K, V>> records,
            Processor<List<ConsumerRecord<K, V>>> processor,
            Acknowledgment ack){

        if(records == null) throw new IllegalArgumentException("records must not be null");
        if(processor == null) throw new IllegalArgumentException("processor must not be null");
        if(ack == null) throw new IllegalArgumentException("ack must not be null");

        int errorCount = 0;
        boolean errorLoop;

        do{
            try{
                processor.proccess(records);
                ack.acknowledge();
                errorLoop = false;
            }catch (Exception e){
                errorLoop = true;
                ++errorCount;
                log.warn("Error while processing records! Retry " + errorCount, e);

                // TODO Report error

                try {
                    // Error Backoff
                    Thread.sleep(Math.min(errorCount * 1000, 1000*60));

                    // TODO Report Stuck in loop
                } catch (InterruptedException e1) { }
            }

        } while (errorLoop);
    }


    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private <K,V> ConsumerRecord<K, V> convertRecord(ConsumerRecord<K, Json> record, Class<V> clazz){
        if(record.value() != null){
            try{
                V value = record.value().json(clazz);
                return ConsumerRecordBuilder.fromRecordWithValue(record, null);
            }catch (JsonParseException e){
                // Not even valid json! Assume poison message.
                reporter.reportPoisonRecordSkipped(record, e); // Report
                return null; // Skip
            }catch (JsonMappingException e) {
                // TODO Maybe add switch to support skipping json mapping errors aswell
                throw e; // For now just rethrow
            }
        }else{
            return ConsumerRecordBuilder.fromRecordWithValue(record, null);
        }
    }


}
