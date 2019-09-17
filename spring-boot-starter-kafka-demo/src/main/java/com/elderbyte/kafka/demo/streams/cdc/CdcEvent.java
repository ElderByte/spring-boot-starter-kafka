package com.elderbyte.kafka.demo.streams.cdc;

import com.elderbyte.messaging.annotations.MessageKey;
import com.elderbyte.messaging.annotations.MessageMetadata;

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

    @MessageMetadata
    public boolean delete = false;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    protected CdcEvent() {}

    /**
     * Creates a new CdcEvent
     */
    public CdcEvent(int id, T previous, T updated, boolean delete) {
        this.id = id;
        this.previous = previous;
        this.updated = updated;
        this.delete = delete;
    }

    @Override
    public String toString() {
        return "CdcEvent{" +
                "id=" + id +
                ", previous=" + previous +
                ", updated=" + updated +
                ", delete=" + delete +
                '}';
    }
}
