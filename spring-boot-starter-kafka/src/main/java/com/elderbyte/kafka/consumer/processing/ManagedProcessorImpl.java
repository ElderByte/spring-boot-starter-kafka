package com.elderbyte.kafka.consumer.processing;

import com.elderbyte.kafka.consumer.ConsumerRecordBuilder;
import com.elderbyte.kafka.metrics.MetricsContext;
import com.elderbyte.kafka.metrics.MetricsReporter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

@SuppressWarnings("Duplicates")
public class ManagedProcessorImpl<K,V> implements ManagedProcessor<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final Logger log = LoggerFactory.getLogger(ManagedProcessorImpl.class);

    private final KafkaProcessorConfiguration<K,V> configuration;
    private final MetricsReporter reporter;
    private final MetricsContext metricsCtx;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/


    public ManagedProcessorImpl(
            KafkaProcessorConfiguration<K,V> configuration,
            MetricsReporter reporter
    ){
      this.configuration = configuration;
      this.reporter = reporter;
      this.metricsCtx = configuration.getMetricsContext();
    }

    /***************************************************************************
     *                                                                         *
     * Public Api                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public void processMessages(List<ConsumerRecord<byte[], byte[]>> rawRecords, Acknowledgment ack, Consumer<?, ?> consumer) {

        long start = System.nanoTime();

        // decode records
        var records = decodeAllRecords(rawRecords);

        // If we are here we have converted the records. Now run the user processing code.

        boolean success;

        if(skipOnAllErrors()){
            success = processAllSkipOnError(records, configuration.getProcessor(), ack);
        }else{
            processAllErrorLoop(records, configuration.getProcessor(), ack);
            success = true;
        }

        if(success){
            reporter.reportStreamingMetrics(metricsCtx, records.size(), System.nanoTime() - start);
        }

        // retry logic

        // health check ??
    }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    public boolean skipOnDecodingErrors(){
        return true;
    }

    public boolean skipOnAllErrors(){
        return true;
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


    private List<ConsumerRecord<K, V>> decodeAllRecords(List<ConsumerRecord<byte[], byte[]>> rawRecords) throws UnrecoverableProcessingException {
        List<ConsumerRecord<K, V>> records;

        try {
            records = rawRecords.stream()
                    .map(this::decodeRecord)
                    .filter(Objects::nonNull) // Skipped records will be null
                    .collect(toList());
        }catch (Exception e){

            // In case a json record failed to map to our dto and don't skip those, its over.

            reporter.reportUnrecoverableCrash(metricsCtx, rawRecords, e);
            throw new UnrecoverableProcessingException("Failed to parse/convert record", e);
        }
        return records;
    }


    private K deserializeKey(ConsumerRecord<byte[], byte[]> record){
        try {
            return configuration.getKeyDeserializer().deserialize(record.topic(), record.key());
        }catch (Exception e){
            throw new IllegalStateException("Failed to deserialize record key!(" + record.serializedKeySize() + ")", e);
        }
    }

    private V deserializeValue(ConsumerRecord<byte[], byte[]> record){
        try {
            return configuration.getValueDeserializer().deserialize(record.topic(), record.value());
        }catch (Exception e){
            throw new IllegalStateException("Failed to deserialize record value! (" + record.serializedValueSize() + ")", e);
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
