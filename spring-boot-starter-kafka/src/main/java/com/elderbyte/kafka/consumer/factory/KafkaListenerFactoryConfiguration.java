package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.config.KafkaClientConfig;
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
    public KafkaListenerFactory kafkaListenerFactory(KafkaClientConfig globalConfig, ObjectMapper mapper, MetricsReporter reporter){
        return new KafkaListenerFactoryImpl(globalConfig, mapper, reporter);
    }

    @Bean
    @ConditionalOnMissingBean(MetricsReporter.class)
    public MetricsReporter metricsReporterLocal(){
        return new MetricsReporterLocal();
    }
}
