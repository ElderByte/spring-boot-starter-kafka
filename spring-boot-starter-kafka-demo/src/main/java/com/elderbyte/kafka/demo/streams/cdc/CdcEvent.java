package com.elderbyte.kafka.demo.streams.cdc;

import com.elderbyte.messaging.annotations.MessageHeader;
import com.elderbyte.messaging.annotations.MessageKey;

public class CdcEvent<T> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    @MessageKey
    public String id;

    public T previous;
    public T updated;

    public boolean delete = false;

    @MessageHeader("cdc.greetings")
    public String greetings;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    protected CdcEvent() {}

    /**
     * Creates a new CdcEvent
     */
    public CdcEvent(
            int id,
            T previous,
            T updated,
            boolean delete,
            String greetings
    ) {
        this.id = id + "";
        this.previous = previous;
        this.updated = updated;
        this.delete = delete;
        this.greetings = greetings;
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
