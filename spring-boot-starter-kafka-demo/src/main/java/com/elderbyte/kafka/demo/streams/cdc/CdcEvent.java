package com.elderbyte.kafka.demo.streams.cdc;

import com.elderbyte.messaging.annotations.MessageKey;

public class CdcEvent<T> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    @MessageKey
    public int id;

    public T previous;
    public T updated;

    public boolean delete = false;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new CdcEvent
     */
    public CdcEvent(int id, T previous, T updated, boolean delete) {
        this.id = id;
        this.previous = previous;
        this.updated = updated;
        this.delete = delete;
    }

}
