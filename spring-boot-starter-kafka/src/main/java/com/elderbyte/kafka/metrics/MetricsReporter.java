package com.elderbyte.kafka.metrics;

import com.elderbyte.kafka.serialisation.Json;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;

public interface MetricsReporter {

    void reportStreamingMetrics(int recordCount, long durationMs);

    void reportMalformedRecord(ConsumerRecord<?, ?> record, Exception e);

    <K> void reportUnrecoverableCrash(Collection<ConsumerRecord<K, Json>> rawRecords, Exception e);

    <K, V> void reportProcessingError(Collection<ConsumerRecord<K,V>> records, Exception e);

    <K, V> void reportProcessingError(Collection<ConsumerRecord<K,V>> records, Exception e, int errorLoopIteration);
}
