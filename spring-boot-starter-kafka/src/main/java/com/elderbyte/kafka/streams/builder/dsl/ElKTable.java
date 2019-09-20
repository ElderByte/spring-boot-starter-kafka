package com.elderbyte.kafka.streams.builder.dsl;

import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Suppressed;

public class ElKTable<K,V> extends ElStreamBase<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KTable<K,V> ktable;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElKtable
     */
    public ElKTable(KTable<K,V> ktable, ElStreamsBuilder<K,V> elBuilder) {
        super(elBuilder);
        this.ktable = ktable;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    public KTable<K, V> ktable() {
        return ktable;
    }

    public ElKTable<K,V> suppress(final Suppressed<? super K> suppressed){
        return builder().el(ktable().suppress(suppressed));
    }

    public ElKStream<K,V> toStream(){
        return builder().el(ktable().toStream());
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
