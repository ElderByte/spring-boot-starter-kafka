package com.elderbyte.kafka.streams.factory;


import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilder;

public interface KafkaStreamsContextBuilderFactory {


    KafkaStreamsContextBuilder newStreamsBuilder(String appName);

}
