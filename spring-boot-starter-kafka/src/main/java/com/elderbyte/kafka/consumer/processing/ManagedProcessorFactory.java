package com.elderbyte.kafka.consumer.processing;

import com.elderbyte.kafka.metrics.MetricsReporter;

/**
 * Provides the ability to build managed processors
 */
public class ManagedProcessorFactory {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final MetricsReporter reporter;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ManagedProcessorFactory with the given reporting backend.
     */
    public ManagedProcessorFactory(MetricsReporter reporter){
        this.reporter = reporter;
    }

    /***************************************************************************
     *                                                                         *
     * Public Api                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a managed processor which will stay in an error loop if the business logic throws an exception.
     *
     * If the json parsing fails, the record will be skipped.
     * If the json mapping fails, a UnrecoverableProcessingException will be thrown.
     *
     * @param <K> The key type
     * @param <V> The value type
     * @return A managed processor ready for duty.
     */
    public <K,V> ManagedProcessor<K, V> buildWithConfiguration(KafkaProcessorConfiguration<K,V> configuration){
        return build(configuration);
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private  <K,V> ManagedProcessor<K, V> build(
            KafkaProcessorConfiguration<K,V> configuration
    ){
        return new ManagedProcessorImpl<>(
                configuration,
                this.reporter
        );
    }

}
