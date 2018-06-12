package com.elderbyte.kafka.consumer.factory;

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
public class KafkaListenerBuilder<K,V> {

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

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    KafkaListenerBuilder(
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

    public <NV> KafkaListenerBuilder<K,NV> valueDeserializer(Deserializer<NV> valueDeserializer){
        return new KafkaListenerBuilder<>(
                this.managedListenerBuilder,
                this.containerProperties,
                this.mapper,
                this.keyDeserializer,
                valueDeserializer
        );
    }

    public <NK> KafkaListenerBuilder<NK,V> keyDeserializer(Deserializer<NK> keyDeserializer){
        return new KafkaListenerBuilder<>(
                this.managedListenerBuilder,
                this.containerProperties,
                this.mapper,
                keyDeserializer,
                this.valueDeserializer
        );
    }


    /***************************************************************************
     *                                                                         *
     * Builder end API                                                         *
     *                                                                         *
     **************************************************************************/


    public void startProcess(Processor<ConsumerRecord<K, V>> processor){
        managedListenerBuilder.buildListener(this, processor)
                .start();
    }

    public void startProcessBatch(Processor<List<ConsumerRecord<K, V>>> processor){
        managedListenerBuilder.buildBatchListener(this, processor)
                .start();
    }

    /***************************************************************************
     *                                                                         *
     * Internal                                                                *
     *                                                                         *
     **************************************************************************/

    ContainerProperties getContainerProperties() {
        return containerProperties;
    }

    boolean isManualAck(){
        return getContainerProperties().getAckMode() == AbstractMessageListenerContainer.AckMode.MANUAL
                || getContainerProperties().getAckMode() == AbstractMessageListenerContainer.AckMode.MANUAL_IMMEDIATE;
    }

}
