package com.elderbyte.kafka.streams;

import com.elderbyte.kafka.config.KafkaClientProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.CleanupConfig;

import java.time.Duration;
import java.util.HashMap;

@Configuration
public class ElderKafkaStreamsConfiguration {

    @Autowired
    private KafkaClientProperties properties;

    @Bean
    public StreamsBuilderFactoryBean streamsBuilderFactoryBean(
            @Autowired KafkaStreamsConfiguration streamsConfig
    ) {
        return new StreamsBuilderFactoryBean(
                streamsConfig,
                new CleanupConfig(false, true));
    }

    @Bean
    public KafkaStreamsConfiguration kafkaStreamsConfiguration(){
        var config = new HashMap<String, Object>();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "_KSTREAM-app1"); // TODO
        config.put(StreamsConfig.CLIENT_ID_CONFIG, "_KSTREAM-app1"); // TODO
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getServers());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Duration.ofSeconds(1).toMillis());

        return new KafkaStreamsConfiguration(config);
    }

}
