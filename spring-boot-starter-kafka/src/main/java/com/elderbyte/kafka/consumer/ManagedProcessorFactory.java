package com.elderbyte.kafka.consumer;

import com.elderbyte.kafka.metrics.MetricsReporter;

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

    public ManagedProcessorFactory(MetricsReporter reporter){
        this.reporter = reporter;
    }

    /***************************************************************************
     *                                                                         *
     * Public Api                                                              *
     *                                                                         *
     **************************************************************************/

    public <K,V> ManagedJsonProcessor<K, V> buildWithErrorLoop(Class<V> valueClazz){
        return build(valueClazz, false, false);
    }

    public <K,V> ManagedJsonProcessor<K, V> buildWithErrorLoop(Class<V> valueClazz, boolean skipOnDtoMappingError ){
        return build(valueClazz, false, skipOnDtoMappingError);
    }

    public <K,V> ManagedJsonProcessor<K, V> buildSkipping(Class<V> valueClazz){
        return build(valueClazz, true, true);
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private  <K,V> ManagedJsonProcessor<K, V> build(Class<V> valueClazz, boolean skipOnError, boolean skipOnDtoMappingError){
        return new ManagedJsonProcessor<>(
                valueClazz,
                skipOnError,
                skipOnDtoMappingError,
                this.reporter
        );
    }


}
