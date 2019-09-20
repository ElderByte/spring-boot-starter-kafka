package com.elderbyte.kafka.streams.builder.dsl;


import org.apache.kafka.streams.kstream.GlobalKTable;

public class ElGlobalKTable<K,V> extends ElStreamBase<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final GlobalKTable<K,V> table;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElKtable
     */
    public ElGlobalKTable(GlobalKTable<K,V> table, ElStreamsBuilder<K,V> elBuilder) {
        super(elBuilder);
        this.table = table;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    public GlobalKTable<K, V> table() {
        return table;
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
