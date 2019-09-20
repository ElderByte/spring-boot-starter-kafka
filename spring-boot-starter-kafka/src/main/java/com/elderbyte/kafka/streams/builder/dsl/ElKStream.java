package com.elderbyte.kafka.streams.builder.dsl;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.TopicNameExtractor;

public class ElKStream<K,V> extends ElStreamBase<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KStream<K,V> kstream;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElKtable
     */
    public ElKStream(KStream<K,V> kstream, ElStreamsBuilder<K,V> elBuilder) {
        super(elBuilder);
        this.kstream = kstream;
    }

    /***************************************************************************
     *                                                                         *
     * Grouping API                                                            *
     *                                                                         *
     **************************************************************************/

    public ElKGroupedStream<K,V> groupByKey(){
        return new ElKGroupedStream<>(
                builder(),
                kstream.groupByKey(serde().grouped())
        );
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Get the underlying KStream
     */
    public KStream<K, V> kstream() {
        return kstream;
    }

    public ElKStream<K,V> peek(final ForeachAction<? super K, ? super V> action){
        kstream.peek(action);
        return this;
    }

    public ElKStream<K,V> merge(ElKStream<K,V> otherStream){
        return builder().el(kstream.merge(otherStream.kstream()));
    }

    public void to(TopicNameExtractor<K, V> topicExtractor){
        kstream.to(topicExtractor, serde().produced());
    }

    public void to(String topicName){
        kstream.to(topicName, serde().produced());
    }

    public ElKStream<K,V> through(String topicName){
        return builder().el(
                kstream.through(topicName, serde().produced())
        );
    }


    /***************************************************************************
     *                                                                         *
     * MAP  API                                                                *
     *                                                                         *
     **************************************************************************/

    public <KR,VR> ElKStreamMapper<K,V, KR, VR> mapTo(KStreamSerde<KR, VR> newSerde){
        return new ElKStreamMapper<>(this, newSerde);
    }

    public <VR> ElKStreamMapper<K,V, K, VR> mapTo(Serde<VR> newValue){
        return new ElKStreamMapper<>(this, serde().withValue(newValue));
    }

    /***************************************************************************
     *                                                                         *
     * Join API                                                                *
     *                                                                         *
     **************************************************************************/


    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
