package com.elderbyte.kafka.metrics;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;

public interface MetricsReporter {

    void reportStreamingMetrics(MetricsContext context, int recordCount, long durationNano);

    void reportMalformedRecord(MetricsContext context, ConsumerRecord<byte[], byte[]> record, Exception e);

    void reportUnrecoverableCrash(MetricsContext context, Collection<ConsumerRecord<byte[], byte[]>> rawRecords, Exception e);

    <K, V> void reportProcessingError(MetricsContext context, Collection<ConsumerRecord<K,V>> records, Exception e);

    <K, V> void reportProcessingError(MetricsContext context, Collection<ConsumerRecord<K,V>> records, Exception e, int errorLoopIteration);
}
