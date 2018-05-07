package com.elderbyte.kafka.metrics;

import com.elderbyte.kafka.serialisation.Json;
import com.elderbyte.kafka.serialisation.JsonParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MetricsReporter {

    void reportPoisonRecordSkipped(ConsumerRecord<?, ?> record, JsonParseException e);
}
