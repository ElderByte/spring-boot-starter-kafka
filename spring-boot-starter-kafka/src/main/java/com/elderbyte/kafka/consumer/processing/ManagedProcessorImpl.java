package com.elderbyte.kafka.consumer.processing;

import com.elderbyte.kafka.metrics.MetricsContext;
import com.elderbyte.kafka.metrics.MetricsReporter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;


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

    private RecordBatchDecoder<K,V> recordBatchDecoder;

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

      this.recordBatchDecoder = new RecordBatchDecoder<>(
              reporter,
              metricsCtx,
              configuration.getKeyDeserializer(),
              configuration.getValueDeserializer()
      );
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
        var records = recordBatchDecoder.decodeAllRecords(rawRecords);

        // If we are here we have converted the records. Now run the user processing code.

        boolean success;

        if(skipOnAllErrors()){
            success = processAllSkipOnError(records, configuration.getProcessor(), ack);
        }else{
            processAllErrorHandler(records, configuration.getProcessor(), ack, configuration.getBlockingRetries(), null);
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



    private void processAllErrorHandler(
            List<ConsumerRecord<K, V>> records,
            Processor<List<ConsumerRecord<K, V>>> processor,
            Acknowledgment ack,
            int blockingRetryAttempts,
            ProcessingErrorHandler<K,V> errorHandler){

        if(records == null) throw new IllegalArgumentException("records must not be null");
        if(processor == null) throw new IllegalArgumentException("processor must not be null");
        if(ack == null) throw new IllegalArgumentException("ack must not be null");

        int errorLoopIteration = 0;
        int retryRemaining = blockingRetryAttempts;

        boolean success = false;
        do{
            try{
                processor.proccess(records);
                ack.acknowledge();
                retryRemaining = 0;
                success = true;
            }catch (Exception e){
                retryRemaining--; // Maybe check for bad health of sink system and block until healthy
                ++errorLoopIteration;
                log.warn("Error while processing records! Retries remaining: " + retryRemaining + " of " + blockingRetryAttempts, e);
                reporter.reportProcessingError(metricsCtx, records, e, errorLoopIteration);
                try {
                    // Error Backoff
                    Thread.sleep(Math.min(errorLoopIteration * 1000, 1000*60));
                } catch (InterruptedException e1) { }
            }
        } while (!success && retryRemaining > 0);

        if(!success){
            // Unsucessful, and all retries have been used
            // TODO -> invoke error handler
            if(errorHandler != null) { errorHandler.handleError(records); }
            ack.acknowledge(); // Skip after delegating error
        }
    }
}
