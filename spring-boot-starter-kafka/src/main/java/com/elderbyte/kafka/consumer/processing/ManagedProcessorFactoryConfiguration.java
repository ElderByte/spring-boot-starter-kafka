package com.elderbyte.kafka.consumer.processing;

import com.elderbyte.kafka.metrics.MetricsReporter;
import com.elderbyte.kafka.metrics.MetricsReporterLocal;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ManagedProcessorFactoryConfiguration {

    @Bean
    public ManagedProcessorFactory managedProcessorFactory(MetricsReporter reporter){
        return new ManagedProcessorFactory(reporter);
    }

    @Bean
    @ConditionalOnMissingBean(MetricsReporter.class)
    public MetricsReporter metricsReporterMock(){
        return new MetricsReporterLocal();
    }
}
