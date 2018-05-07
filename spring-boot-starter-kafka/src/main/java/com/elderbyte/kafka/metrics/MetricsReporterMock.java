package com.elderbyte.kafka.metrics;

import com.elderbyte.kafka.serialisation.Json;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;

public class MetricsReporterMock implements MetricsReporter {

    @Override
    public void reportStreamingMetrics(MetricsContext context, int recordCount, long durationNano) {

    }

    @Override
    public void reportMalformedRecord(MetricsContext context, ConsumerRecord<?, ?> record, Exception e) {

    }

    @Override
    public <K> void reportUnrecoverableCrash(MetricsContext context, Collection<ConsumerRecord<K, Json>> rawRecords, Exception e) {

    }

    @Override
    public <K, V> void reportProcessingError(MetricsContext context, Collection<ConsumerRecord<K, V>> consumerRecords, Exception e) {

    }

    @Override
    public <K, V> void reportProcessingError(MetricsContext context, Collection<ConsumerRecord<K, V>> consumerRecords, Exception e, int errorLoopIteration) {

    }
}
