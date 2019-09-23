package com.elderbyte.kafka.streams.factory;

import com.elderbyte.kafka.config.KafkaClientProperties;
import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilder;
import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilderImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.CleanupConfig;

import java.util.HashMap;

public class KafkaStreamsContextFactoryImpl implements KafkaStreamsContextBuilderFactory {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KafkaClientProperties properties;
    private final ObjectMapper mapper;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new KafkaStreamsBuilderFactory
     */
    public KafkaStreamsContextFactoryImpl(
            KafkaClientProperties properties,
            ObjectMapper mapper
    ) {
        this.properties = properties;
        this.mapper = mapper;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public KafkaStreamsContextBuilder newStreamsBuilder(String appName) {
        return new KafkaStreamsContextBuilderImpl(
                mapper,
                kafkaStreamsConfiguration(appName)
        );
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/


    private KafkaStreamsConfiguration kafkaStreamsConfiguration(String appName){

        // https://docs.confluent.io/current/streams/developer-guide/config-streams.html

        var streams = properties.getStreams();

        var config = new HashMap<String, Object>();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "_KSTREAM-" + appName);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getServers());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, streams.getOptimizeTopology());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, streams.getCommitInterval().toMillis());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, streams.getProcessingGuarantee()); // AT_LEAST_ONCE default

        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, streams.getReplicationFactor()); // Default 1, set to 3 for HA
        config.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), streams.getProducerAcks()); // all for HA
        config.put(StreamsConfig.STATE_DIR_CONFIG, streams.getStateDir());

        return new KafkaStreamsConfiguration(config);
    }


}
