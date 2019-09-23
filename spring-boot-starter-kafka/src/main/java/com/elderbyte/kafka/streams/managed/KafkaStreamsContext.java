package com.elderbyte.kafka.streams.managed;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.context.SmartLifecycle;

import java.util.Optional;

public interface KafkaStreamsContext extends SmartLifecycle {

    Topology getTopology();

    Optional<KafkaStreams> getKafkaStreams();
}
