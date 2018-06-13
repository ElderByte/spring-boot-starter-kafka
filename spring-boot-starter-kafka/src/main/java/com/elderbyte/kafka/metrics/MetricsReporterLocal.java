package com.elderbyte.kafka.metrics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MetricsReporterLocal implements MetricsReporter {

    private final Logger log = LoggerFactory.getLogger(MetricsReporterLocal.class);

    @Override
    public void reportStreamingMetrics(MetricsContext context, int recordCount, long durationNano) {
        if(log.isDebugEnabled()){
            var durationMs = durationNano / (1000 * 1000);
            log.debug(formatContextHeader(context) + ": Batch of "+recordCount+" records processed in " + durationMs + "ms." );
        }
    }

    @Override
    public void reportMalformedRecord(MetricsContext context, ConsumerRecord<byte[], byte[]> record, Exception e) {
        log.warn(formatContextHeader(context) + ": Failed to decode record: " + record.toString(), e);
    }

    @Override
    public void reportUnrecoverableCrash(MetricsContext context, Collection<ConsumerRecord<byte[], byte[]>> rawRecords, Exception e) {
        log.error(formatContextHeader(context) + ": Failed hard for " + rawRecords.stream() + " records.", e);

    }

    @Override
    public <K, V> void reportProcessingError(MetricsContext context, Collection<ConsumerRecord<K, V>> consumerRecords, Exception e) {
        log.warn(formatContextHeader(context) + ": Failed to process records: " + consumerRecords.toString(), e);
    }

    @Override
    public <K, V> void reportProcessingError(MetricsContext context, Collection<ConsumerRecord<K, V>> consumerRecords, Exception e, int errorLoopIteration) {
        log.warn(formatContextHeader(context) + ": Failed to process records: " + consumerRecords.toString() + ". Error Loop Iteration: " + errorLoopIteration, e);
    }


    /**
     * Format the context header
     */
    private String formatContextHeader(MetricsContext context){
        return context.getAppId() + "-" + context.getInstanceId();
    }
}
