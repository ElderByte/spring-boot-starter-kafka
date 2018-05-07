package com.elderbyte.kafka.metrics;

import com.elderbyte.kafka.serialisation.Json;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;

public interface MetricsReporter {

    void reportStreamingMetrics(MetricsContext context, int recordCount, long durationNano);

    void reportMalformedRecord(MetricsContext context, ConsumerRecord<?, ?> record, Exception e);

    <K> void reportUnrecoverableCrash(MetricsContext context, Collection<ConsumerRecord<K, Json>> rawRecords, Exception e);

    <K, V> void reportProcessingError(MetricsContext context, Collection<ConsumerRecord<K,V>> records, Exception e);

    <K, V> void reportProcessingError(MetricsContext context, Collection<ConsumerRecord<K,V>> records, Exception e, int errorLoopIteration);
}
