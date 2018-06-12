package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.config.KafkaClientConfig;
import com.elderbyte.kafka.consumer.factory.listeners.ManagedAckBatchRawListener;
import com.elderbyte.kafka.consumer.factory.listeners.ManagedAckRawListener;
import com.elderbyte.kafka.consumer.factory.listeners.ManagedBatchRawListener;
import com.elderbyte.kafka.consumer.factory.listeners.ManagedRawListener;
import com.elderbyte.kafka.consumer.processing.Processor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@SuppressWarnings("Duplicates")
public class KafkaListenerFactoryImpl implements KafkaListenerFactory, ManagedListenerBuilder {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    @Autowired
    private KafkaClientConfig config;

    @Autowired
    private ObjectMapper mapper;

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

    public <K,V> GenericMessageListenerContainer<byte[], byte[]> buildListener(KafkaListenerBuilder<K,V> builder, Processor<ConsumerRecord<K, V>> processor){
        return buildListenerInternal(builder, buildListener(processor, builder));
    }

    public <K,V> GenericMessageListenerContainer<byte[], byte[]> buildBatchListener(KafkaListenerBuilder<K,V> builder,Processor<List<ConsumerRecord<K, V>>> processor){
        return buildListenerInternal(builder, buildBatchListener(processor, builder));
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private <K,V> GenericMessageListenerContainer<byte[], byte[]> buildListenerInternal(KafkaListenerBuilder<K,V> builder, Object listener){
        var containerProps = builder.getContainerProperties();
        containerProps.setMessageListener(listener);
        var config = defaultConfig();
        // TODO APPLY CONFIG FROM USER
        return new KafkaMessageListenerContainer<>(consumerFactoryByteByte(config), containerProps);
    }

    private KafkaListenerBuilder<byte[], byte[]> startBuilder(ContainerProperties properties) {
        return new KafkaListenerBuilder<>(
                this,
                properties,
                mapper,
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer()
        );
    }

    private <K,V> Object buildListener(Processor<ConsumerRecord<K, V>> processor, KafkaListenerBuilder<K,V> builder){
        return builder.isManualAck() ? new ManagedAckRawListener<>(processor) : new ManagedRawListener<>(processor);
    }

    private <K,V> Object buildBatchListener(Processor<List<ConsumerRecord<K, V>>> processor, KafkaListenerBuilder<K,V> builder){
        return builder.isManualAck() ? new ManagedAckBatchRawListener<>(processor) : new ManagedBatchRawListener<>(processor);
    }

    private ConsumerFactory<byte[], byte[]> consumerFactoryByteByte(Map<String, Object> config) {
        var factory = new DefaultKafkaConsumerFactory<byte[], byte[]>(config);
        factory.setKeyDeserializer(new ByteArrayDeserializer());
        factory.setValueDeserializer(new ByteArrayDeserializer());
        return factory;
    }

    private Map<String, Object> defaultConfig() {
        var props = new HashMap<String, Object>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaServers());
        // props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.isConsumerAutoCommit());
        // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getConsumerAutoOffsetReset());
        config.getConsumerMaxPollRecords().ifPresent(max ->  props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, max));
        return props;
    }

}
