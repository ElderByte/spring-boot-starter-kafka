package com.elderbyte.kafka.demo.streams.cdc;

import com.elderbyte.messaging.annotations.MessageKey;
import com.elderbyte.messaging.annotations.MessageMetadata;
import org.apache.kafka.common.protocol.types.Field;

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

    @MessageMetadata(key = "cdc.greetings")
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
        this.id = id;
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
