package com.elderbyte.kafka.consumer.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;

import java.util.regex.Pattern;

public class KafkaListenerFactoryImpl implements KafkaListenerFactory {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final ManagedListenerBuilder managedListenerBuilder;
    private final ObjectMapper mapper;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    @Autowired
    public KafkaListenerFactoryImpl(ObjectMapper mapper, ManagedListenerBuilder managedListenerBuilder){
        this.mapper = mapper;
        this.managedListenerBuilder = managedListenerBuilder;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public KafkaListenerBuilder<byte[], byte[]> start(String... topics) {
        return startBuilder(new ContainerProperties(topics));
    }

    @Override
    public KafkaListenerBuilder<byte[], byte[]> start(Pattern topicPattern) {
        return startBuilder(new ContainerProperties(topicPattern));
    }

    @Override
    public KafkaListenerBuilder<byte[], byte[]> start(TopicPartitionInitialOffset... topicPartitions) {
        return startBuilder(new ContainerProperties(topicPartitions));
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private KafkaListenerBuilder<byte[], byte[]> startBuilder(ContainerProperties properties) {
        return new KafkaListenerBuilderImpl<>(
                managedListenerBuilder,
                properties,
                mapper,
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer()
        );
    }

}
