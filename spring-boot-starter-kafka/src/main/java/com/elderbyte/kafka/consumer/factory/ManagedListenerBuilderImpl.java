package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.config.KafkaClientConfig;
import com.elderbyte.kafka.consumer.factory.listeners.SpringListenerAdapter;
import com.elderbyte.kafka.consumer.processing.ManagedProcessorImpl;
import com.elderbyte.kafka.metrics.MetricsReporter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.GenericMessageListener;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

public class ManagedListenerBuilderImpl implements ManagedListenerBuilder {

    private final KafkaClientConfig globalConfig;
    private final MetricsReporter reporter;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    @Autowired
    public ManagedListenerBuilderImpl(KafkaClientConfig globalConfig, MetricsReporter reporter){
        this.globalConfig = globalConfig;
        this.reporter = reporter;
    }

    /***************************************************************************
     *                                                                         *
     * Public API methods                                                      *
     *                                                                         *
     **************************************************************************/

    @Override
    public <K,V> GenericMessageListenerContainer<byte[], byte[]> buildListenerContainer(KafkaListenerConfiguration<K,V> configuration){
        var managedProcessor = new ManagedProcessorImpl<>(configuration, reporter);
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
