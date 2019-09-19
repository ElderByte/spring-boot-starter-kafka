package com.elderbyte.kafka.demo.streams.model.orders;

import com.elderbyte.messaging.annotations.Tombstone;

@Tombstone
public class OrderDeletedMessage extends OrderMessage {

    public OrderDeletedMessage(){}

    public OrderDeletedMessage(String number, String company) {
        super(number, company);
    }
}
