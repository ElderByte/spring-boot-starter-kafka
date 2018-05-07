package com.elderbyte.kafka.consumer.processing;

import com.elderbyte.kafka.metrics.MetricsContext;
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
     * @param valueClazz The value class (json conversion target)
     * @param metricsContext The metrics context
     * @param <K> The key type
     * @param <V> The value type
     * @return A managed processor ready for duty.
     */
    public <K,V> ManagedJsonProcessor<K, V> buildWithErrorLoop(Class<V> valueClazz, MetricsContext metricsContext){
        return build(valueClazz, metricsContext,false, false);
    }

    /**
     * Creates a managed processor which will stay in an error loop if the business logic throws an exception.
     * @param valueClazz The value class (json conversion target)
     * @param metricsContext The metrics context
     * @param skipOnDtoMappingError Skip records which can not be converted to the requested pojo.
     * @param <K> The key type
     * @param <V> The value type
     * @return A managed processor ready for duty.
     */
    public <K,V> ManagedJsonProcessor<K, V> buildWithErrorLoop(Class<V> valueClazz, MetricsContext metricsContext, boolean skipOnDtoMappingError ){
        return build(valueClazz, metricsContext,false, skipOnDtoMappingError);
    }

    /**
     * Creates a managed processor which will skip records which produce errors.
     * @param valueClazz The value class (json conversion target)
     * @param metricsContext The metrics context
     * @param <K> The key type
     * @param <V> The value type
     * @return A managed processor ready for duty.
     */
    public <K,V> ManagedJsonProcessor<K, V> buildSkipping(Class<V> valueClazz, MetricsContext metricsContext){
        return build(valueClazz, metricsContext,true, true);
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private  <K,V> ManagedJsonProcessor<K, V> build(
            Class<V> valueClazz,
            MetricsContext metricsContext,
            boolean skipOnError,
            boolean skipOnDtoMappingError
    ){
        return new ManagedJsonProcessor<>(
                valueClazz,
                skipOnError,
                skipOnDtoMappingError,
                this.reporter,
                metricsContext
        );
    }


}
