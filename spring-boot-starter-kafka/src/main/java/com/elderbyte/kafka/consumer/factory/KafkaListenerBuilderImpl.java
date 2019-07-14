package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.consumer.configuration.AutoOffsetReset;
import com.elderbyte.kafka.consumer.processing.Processor;
import com.elderbyte.kafka.metrics.MetricsContext;
import com.elderbyte.kafka.serialisation.SpringKafkaJsonDeserializer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.Collection;
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
    private MetricsContext metricsContext = MetricsContext.from("", "");
    private boolean skipOnError = false;
    private int blockingRetries = 1;
    private boolean failIfTopicsAreMissing = false;

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

        autoCommit(false);
    }

    /***************************************************************************
     *                                                                         *
     * Builder API                                                             *
     *                                                                         *
     **************************************************************************/

    @Override
    public <NV> KafkaListenerBuilder<K,NV> jsonValue(Class<NV> valueClazz){
        return valueDeserializer(new SpringKafkaJsonDeserializer<>(valueClazz, mapper));
    }

    @Override
    public <NK> KafkaListenerBuilder<NK,V> jsonKey(Class<NK> keyClazz){
        return keyDeserializer(new SpringKafkaJsonDeserializer<>(keyClazz, mapper));
    }

    @Override
    public <NV> KafkaListenerBuilder<K,NV> jsonValue(TypeReference<NV> valueTypeRef){
        return valueDeserializer(new SpringKafkaJsonDeserializer<>(valueTypeRef, mapper));
    }

    @Override
    public <NK> KafkaListenerBuilder<NK,V> jsonKey(TypeReference<NK> keyTypeRef){
        return keyDeserializer(new SpringKafkaJsonDeserializer<>(keyTypeRef, mapper));
    }

    @Override
    public KafkaListenerBuilder<K,String> stringValue(){
        return valueDeserializer(new StringDeserializer());
    }

    @Override
    public KafkaListenerBuilder<String,V> stringKey(){
        return keyDeserializer(new StringDeserializer());
    }

    @Override
    public <NV> KafkaListenerBuilder<K,NV> valueDeserializer(Deserializer<NV> valueDeserializer){
        return new KafkaListenerBuilderImpl<>(
                this.managedListenerBuilder,
                this.containerProperties,
                this.mapper,
                this.keyDeserializer,
                valueDeserializer
        ).apply(this);
    }

    @Override
    public <NK> KafkaListenerBuilder<NK,V> keyDeserializer(Deserializer<NK> keyDeserializer){
        return new KafkaListenerBuilderImpl<>(
                this.managedListenerBuilder,
                this.containerProperties,
                this.mapper,
                keyDeserializer,
                this.valueDeserializer
        ).apply(this);
    }

    @Override
    public KafkaListenerBuilder<K,V>  autoOffsetReset(AutoOffsetReset autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
        return this;
    }

    public KafkaListenerBuilder<K,V> failIfTopicsAreMissing(boolean value){
        this.failIfTopicsAreMissing = value;
        return this;
    }

    @Override
    public KafkaListenerBuilder<K,V> consumerGroup(String groupId){
        this.containerProperties.setGroupId(groupId);
        return this;
    }

    @Override
    public KafkaListenerBuilder<K,V> consumerId(String clientId){
        this.containerProperties.setClientId(clientId);
        return this;
    }

    @Override
    public KafkaListenerBuilder<K,V> metrics(MetricsContext metricsContext){
        this.metricsContext = metricsContext;
        return this;
    }

    @Override
    public KafkaListenerBuilder<K,V> ignoreErrors() {
        this.skipOnError = true;
        return this;
    }

    @Override
    public KafkaListenerBuilder<K, V> blockingRetries(int retries) {
        this.blockingRetries = retries;
        return this;
    }

    @Override
    public KafkaListenerBuilder<K, V> autoCommit(boolean autoCommit) {
        this.containerProperties.setAckMode(autoCommit ? ContainerProperties.AckMode.BATCH : ContainerProperties.AckMode.MANUAL);
        return this;
    }

    @Override
    public KafkaListenerBuilder<K, V> rebalanceListener(ConsumerAwareRebalanceListener rebalanceListener){
        this.containerProperties.setConsumerRebalanceListener(rebalanceListener);
        return this;
    }

    @Override
    public KafkaListenerBuilder<K, V> seekToEarliest(){
        rebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                consumer.seekToBeginning(partitions);
            }
        });
        return this;
    }

    @Override
    public KafkaListenerBuilder<K, V> seekToLatest(){
        rebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                consumer.seekToEnd(partitions);
            }
        });
        return this;
    }


    @Override
    public KafkaListenerBuilder<K, V> syncCommits(boolean syncCommits){
        this.containerProperties.setSyncCommits(syncCommits);
        return this;
    }

    @Override
    public KafkaListenerBuilder<K, V> pollTimeout(long pollTimeout){
        this.containerProperties.setPollTimeout(pollTimeout);
        return this;
    }

    public KafkaListenerBuilder<K,V> apply(KafkaListenerConfiguration<?,?> prototype){
        this.autoOffsetReset = prototype.getAutoOffsetReset();
        this.metricsContext = prototype.getMetricsContext();
        this.blockingRetries = prototype.getBlockingRetries();
        this.skipOnError = prototype.isIgnoreErrors();
        return this;
    }

    /***************************************************************************
     *                                                                         *
     * Builder end API                                                         *
     *                                                                         *
     **************************************************************************/

    /**
     * Build a single record listener from this builder configuration.
     */
    public MessageListenerContainer build(Processor<ConsumerRecord<K, V>> processor){
        return buildListenerContainer(batch -> {
            for(var e : batch){
                processor.proccess(e);
            }
        });
    }

    /**
     * Build a kafka batch listener from this builder configuration.
     */
    public MessageListenerContainer buildBatch(Processor<List<ConsumerRecord<K, V>>> processor){
        this.batch = true;
        return buildListenerContainer(processor);
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
        return getContainerProperties().getAckMode() == ContainerProperties.AckMode.MANUAL
                || getContainerProperties().getAckMode() == ContainerProperties.AckMode.MANUAL_IMMEDIATE;
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

    @Override
    public boolean failIfTopicsAreMissing() {
        return failIfTopicsAreMissing;
    }

    @Override
    public MetricsContext getMetricsContext() {
        return metricsContext;
    }

    @Override
    public boolean isIgnoreErrors() {
        return skipOnError;
    }

    @Override
    public int getBlockingRetries() {
        return blockingRetries;
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private MessageListenerContainer buildListenerContainer(Processor<List<ConsumerRecord<K, V>>> processor){
        this.processor = processor;
        return managedListenerBuilder.buildListenerContainer(this);
    }
}
