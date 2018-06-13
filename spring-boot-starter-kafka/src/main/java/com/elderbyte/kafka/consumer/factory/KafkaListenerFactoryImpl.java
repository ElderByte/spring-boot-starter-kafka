package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.config.KafkaClientConfig;
import com.elderbyte.kafka.consumer.factory.listeners.*;
import com.elderbyte.kafka.consumer.factory.processor.ManagedProcessorImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.GenericMessageListener;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class KafkaListenerFactoryImpl implements KafkaListenerFactory, ManagedListenerBuilder {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KafkaClientConfig globalConfig;
    private final ObjectMapper mapper;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    @Autowired
    public KafkaListenerFactoryImpl(KafkaClientConfig globalConfig, ObjectMapper mapper){
        this.globalConfig = globalConfig;
        this.mapper = mapper;
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
     * Internal methods                                                        *
     *                                                                         *
     **************************************************************************/

    @Override
    public <K,V> GenericMessageListenerContainer<byte[], byte[]> buildListenerContainer(KafkaListenerConfiguration<K,V> configuration){
        var managedProcessor = new ManagedProcessorImpl<>(configuration);
        var listener = SpringListenerAdapter.buildListenerAdapter(configuration, managedProcessor);
        return buildListenerInternal(configuration, listener);
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private <K,V> GenericMessageListenerContainer<byte[], byte[]> buildListenerInternal(KafkaListenerConfiguration<K,V> config, GenericMessageListener<?> listener){
        var containerProps = config.getContainerProperties();
        containerProps.setMessageListener(listener);
        var kafkaConfig = defaultConfig();

        kafkaConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, !config.isManualAck());
        kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getAutoOffsetReset().toString());

        return new KafkaMessageListenerContainer<>(consumerFactoryByteByte(kafkaConfig), containerProps);
    }

    private KafkaListenerBuilder<byte[], byte[]> startBuilder(ContainerProperties properties) {
        return new KafkaListenerBuilderImpl<>(
                this,
                properties,
                mapper,
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer()
        );
    }

    private ConsumerFactory<byte[], byte[]> consumerFactoryByteByte(Map<String, Object> config) {
        var factory = new DefaultKafkaConsumerFactory<byte[], byte[]>(config);
        factory.setKeyDeserializer(new ByteArrayDeserializer());
        factory.setValueDeserializer(new ByteArrayDeserializer());
        return factory;
    }

    private Map<String, Object> defaultConfig() {
        var props = new HashMap<String, Object>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, globalConfig.getKafkaServers());
        globalConfig.getConsumerMaxPollRecords().ifPresent(max ->  props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, max));
        return props;
    }

}
