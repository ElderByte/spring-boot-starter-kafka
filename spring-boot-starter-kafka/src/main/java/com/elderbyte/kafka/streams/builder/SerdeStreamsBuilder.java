package com.elderbyte.kafka.streams.builder;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import com.elderbyte.kafka.streams.builder.dsl.KStreamSerde;

import java.util.Collection;
import java.util.regex.Pattern;

public class SerdeStreamsBuilder<K, V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final StreamsBuilder streamsBuilder;
    private final KStreamSerde<K,V> streamSerde;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new KafkaStreamsBuilderJson
     */
    public SerdeStreamsBuilder(
            StreamsBuilder streamsBuilder,
            KStreamSerde<K,V> streamSerde
    ) {
        this.streamsBuilder = streamsBuilder;
        this.streamSerde = streamSerde;
    }

    public <KR, VR> SerdeStreamsBuilder<KR, VR> with(KStreamSerde<KR, VR> streamSerde){
        return new SerdeStreamsBuilder<>(streamsBuilder, streamSerde);
    }

    /***************************************************************************
     *                                                                         *
     * KStream                                                                 *
     *                                                                         *
     **************************************************************************/

    public KStream<K, V> kstream(String topic) {
        return streamsBuilder.stream(
                topic,
                serde().consumed()
        );
    }

    public KStream<K, V> kstream(Collection<String> topics) {
        return streamsBuilder.stream(
                topics,
                serde().consumed()
        );
    }

    public KStream<K, V> kstream(Pattern topicPattern) {
        return streamsBuilder.stream(
                topicPattern,
                serde().consumed()
        );
    }

    /***************************************************************************
     *                                                                         *
     * KTables                                                                 *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a KTable from a compacted topic.
     */
    public KTable<K, V> ktable(String compactedTopic, String storeName) {
        return streamsBuilder.table(
                compactedTopic,
                serde().materialized(storeName)
                        .withLoggingDisabled() // Since we are backed by a compacted topic, no need
        );
    }

    /**
     * Creates a GlobalKTable from a compacted topic.
     */
    public GlobalKTable<K, V> globalKTable(String compactedTopic, String storeName) {
        return streamsBuilder.globalTable(
                compactedTopic,
                serde().materialized(storeName)
                        .withLoggingDisabled() // Since we are backed by a compacted topic, no need
        );
    }


    /***************************************************************************
     *                                                                         *
     * Serdes                                                                  *
     *                                                                         *
     **************************************************************************/

    public KStreamSerde<K,V> serde(){
        return streamSerde;
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
