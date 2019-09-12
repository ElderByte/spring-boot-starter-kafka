package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.config.KafkaClientProperties;
import com.elderbyte.kafka.metrics.MetricsReporter;
import com.elderbyte.kafka.metrics.MetricsReporterLocal;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides the ability to build managed processors
 */
@Configuration
public class KafkaListenerFactoryConfiguration {

    @Bean
    public ManagedListenerBuilder managedListenerBuilder(KafkaClientProperties globalConfig, MetricsReporter reporter){
        return new ManagedListenerBuilderImpl(globalConfig, reporter);
    }

    @Bean
    public KafkaListenerFactory kafkaListenerFactory(ObjectMapper mapper, ManagedListenerBuilder managedListenerBuilder){
        return new KafkaListenerFactoryImpl(mapper, managedListenerBuilder);
    }

    @Bean
    @ConditionalOnMissingBean(MetricsReporter.class)
    public MetricsReporter metricsReporterLocal(){
        return new MetricsReporterLocal();
    }
}
