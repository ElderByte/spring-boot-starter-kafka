package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.consumer.configuration.AutoOffsetReset;
import com.elderbyte.kafka.consumer.processing.Processor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.List;


/**
 * Holds the complete configuration of a kafka listener
 */
public class KafkaListenerBuilderImpl<K,V> implements KafkaListenerBuilder<K,V>, KafkaListenerConfiguration<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final ManagedListenerBuilder managedListenerBuilder;

    private final ObjectMapper mapper;

    private final ContainerProperties containerProperties;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;


    private AutoOffsetReset autoOffsetReset = AutoOffsetReset.latest;


    private Processor<List<ConsumerRecord<K, V>>> processor;
    private boolean batch = false;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    KafkaListenerBuilderImpl(
            ManagedListenerBuilder managedListenerBuilder,
            ContainerProperties containerProperties,
            ObjectMapper mapper,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer
    )
    {
        this.managedListenerBuilder = managedListenerBuilder;
        this.containerProperties = containerProperties;
        this.mapper = mapper;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    /***************************************************************************
     *                                                                         *
     * Builder API                                                             *
     *                                                                         *
     **************************************************************************/

    public <NV> KafkaListenerBuilder<K,NV> jsonValue(Class<NV> valueClazz){
        return valueDeserializer(new JsonDeserializer<>(valueClazz, mapper));
    }

    public <NK> KafkaListenerBuilder<NK,V> jsonKey(Class<NK> keyClazz){
        return keyDeserializer(new JsonDeserializer<>(keyClazz, mapper));
    }

    public <NV> KafkaListenerBuilder<K,NV> valueDeserializer(Deserializer<NV> valueDeserializer){
        return new KafkaListenerBuilderImpl<>(
                this.managedListenerBuilder,
                this.containerProperties,
                this.mapper,
                this.keyDeserializer,
                valueDeserializer
        ).apply(this);
    }

    public <NK> KafkaListenerBuilder<NK,V> keyDeserializer(Deserializer<NK> keyDeserializer){
        return new KafkaListenerBuilderImpl<>(
                this.managedListenerBuilder,
                this.containerProperties,
                this.mapper,
                keyDeserializer,
                this.valueDeserializer
        ).apply(this);
    }

    public KafkaListenerBuilder<K,V>  autoOffsetReset(AutoOffsetReset autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
        return this;
    }

    public KafkaListenerBuilder<K,V> consumerGroup(String groupId){
        this.containerProperties.setGroupId(groupId);
        return this;
    }

    public KafkaListenerBuilder<K,V> consumerId(String clientId){
        this.containerProperties.setClientId(clientId);
        return this;
    }

    public KafkaListenerBuilder<K,V> apply(KafkaListenerConfiguration<?,?> prototype){
        this.autoOffsetReset = prototype.getAutoOffsetReset();
        return this;
    }

    /***************************************************************************
     *                                                                         *
     * Builder end API                                                         *
     *                                                                         *
     **************************************************************************/


    public void startProcess(Processor<ConsumerRecord<K, V>> processor){
        startProcessing(batch -> {
            for(var e : batch){
                processor.proccess(e);
            }
        });
    }

    public void startProcessBatch(Processor<List<ConsumerRecord<K, V>>> processor){
        this.batch = true;
        startProcessing(processor);
    }

    private void startProcessing(Processor<List<ConsumerRecord<K, V>>> processor){
        this.processor = processor;
        managedListenerBuilder.buildListenerContainer(this)
                .start();
    }

    /***************************************************************************
     *                                                                         *
     * Configuration                                                           *
     *                                                                         *
     **************************************************************************/

    @Override
    public ContainerProperties getContainerProperties() {
        return containerProperties;
    }

    @Override
    public boolean isManualAck(){
        return getContainerProperties().getAckMode() == AbstractMessageListenerContainer.AckMode.MANUAL
                || getContainerProperties().getAckMode() == AbstractMessageListenerContainer.AckMode.MANUAL_IMMEDIATE;
    }

    @Override
    public Deserializer<K> getKeyDeserializer() {
        return keyDeserializer;
    }

    @Override
    public Deserializer<V> getValueDeserializer() {
        return valueDeserializer;
    }

    @Override
    public Processor<List<ConsumerRecord<K, V>>> getProcessor() {
        return processor;
    }

    @Override
    public boolean isBatch() {
        return batch;
    }

    @Override
    public AutoOffsetReset getAutoOffsetReset() {
        return autoOffsetReset;
    }
}