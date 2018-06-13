package com.elderbyte.kafka.consumer.processing;

import com.elderbyte.kafka.metrics.MetricsContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;

public interface KafkaProcessorConfiguration<K,V> {

    Deserializer<K> getKeyDeserializer();

    Deserializer<V> getValueDeserializer();

    Processor<List<ConsumerRecord<K, V>>> getProcessor();

    MetricsContext getMetricsContext();

}
