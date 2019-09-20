package com.elderbyte.kafka.streams.builder.dsl;

import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilder;
import com.elderbyte.kafka.streams.builder.SerdeStreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Collection;
import java.util.regex.Pattern;

public class ElStreamsBuilder<K,V> {

    /***************************************************************************
     *                                                                         *
     * Static Builder                                                          *
     *                                                                         *
     **************************************************************************/

    public static <K,V> ElStreamsBuilder<K,V> from(
            KafkaStreamsContextBuilder contextBuilder,
            KStreamSerde<K,V> serde
    ){
        return new ElStreamsBuilder<>(
                contextBuilder,
                new SerdeStreamsBuilder<>(contextBuilder.streamsBuilder(), serde)
        );
    }

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KafkaStreamsContextBuilder contextBuilder;
    private final SerdeStreamsBuilder<K, V> serdeStreamsBuilder;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElStreamsBuilder
     */
    private ElStreamsBuilder(
            KafkaStreamsContextBuilder contextBuilder,
            SerdeStreamsBuilder<K, V> serdeStreamsBuilder
    ) {
        this.contextBuilder = contextBuilder;
        this.serdeStreamsBuilder = serdeStreamsBuilder;
    }


    public <KR,VR> ElStreamsBuilder<KR,VR> with(KStreamSerde<KR,VR> streamSerde){
        return new ElStreamsBuilder<>(
                contextBuilder,
                serdeStreamsBuilder.with(streamSerde)
        );
    }

    /***************************************************************************
     *                                                                         *
     * KStream                                                                 *
     *                                                                         *
     **************************************************************************/

    public ElKStream<K, V> kstream(String topic) {
        return el(serdeStreamsBuilder.kstream(topic));
    }

    public ElKStream<K, V> kstream(Collection<String> topics) {
        return el(serdeStreamsBuilder.kstream(topics));
    }

    public ElKStream<K, V> kstream(Pattern topicPattern) {
        return el(serdeStreamsBuilder.kstream(topicPattern));
    }

    /***************************************************************************
     *                                                                         *
     * KTables                                                                 *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a KTable from a compacted topic.
     */
    public ElKTable<K, V> ktable(String compactedTopic, String storeName) {
        return el(serdeStreamsBuilder.ktable(compactedTopic, storeName));
    }

    /**
     * Creates a GlobalKTable from a compacted topic.
     */
    public ElGlobalKTable<K, V> globalKTable(String compactedTopic, String storeName) {
        return el(serdeStreamsBuilder.globalKTable(compactedTopic, storeName));
    }

    /***************************************************************************
     *                                                                         *
     * Serde                                                                   *
     *                                                                         *
     **************************************************************************/

    public KStreamSerde<K,V> serde(){
        return serdeStreamsBuilder.serde();
    }

    public KafkaStreamsContextBuilder context(){
        return contextBuilder;
    }

    /***************************************************************************
     *                                                                         *
     * Wrapper                                                                 *
     *                                                                         *
     **************************************************************************/

    public ElKStream<K, V> el(KStream<K,V> stream){
        return new ElKStream<>(stream, this);
    }

    public ElKTable<K, V> el(KTable<K,V> table){
        return new ElKTable<>(table, this);
    }

    public ElGlobalKTable<K, V> el(GlobalKTable<K,V> table){
        return new ElGlobalKTable<>(table, this);
    }
}
