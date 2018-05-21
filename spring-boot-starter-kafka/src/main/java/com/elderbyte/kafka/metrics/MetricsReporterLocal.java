package com.elderbyte.kafka.metrics;

import com.elderbyte.kafka.consumer.processing.ManagedJsonProcessor;
import com.elderbyte.kafka.serialisation.Json;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MetricsReporterLocal implements MetricsReporter {

    private final Logger log = LoggerFactory.getLogger(ManagedJsonProcessor.class);


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
