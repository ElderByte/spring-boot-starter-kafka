package com.elderbyte.kafka.streams.factory;

import com.elderbyte.kafka.config.KafkaClientProperties;
import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilder;
import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilderImpl;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.CleanupConfig;

import java.time.Duration;
import java.util.HashMap;

public class KafkaStreamsContextFactoryImpl implements KafkaStreamsContextBuilderFactory {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KafkaClientProperties properties;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new KafkaStreamsBuilderFactory
     */
    public KafkaStreamsContextFactoryImpl(
            KafkaClientProperties properties
    ) {
        this.properties = properties;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public KafkaStreamsContextBuilder newStreamsBuilder(String appName) {
        return new KafkaStreamsContextBuilderImpl(
                kafkaStreamsConfiguration(appName),
                new CleanupConfig(false, true)

        );
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private KafkaStreamsConfiguration kafkaStreamsConfiguration(String appName){
        var config = new HashMap<String, Object>();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "_KSTREAM-" + appName);
        config.put(StreamsConfig.CLIENT_ID_CONFIG, "_KSTREAM-" + appName);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getServers());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Duration.ofSeconds(1).toMillis());
        return new KafkaStreamsConfiguration(config);
    }


}
