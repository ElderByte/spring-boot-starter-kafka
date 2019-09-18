package com.elderbyte.kafka.demo.streams.model;

import com.elderbyte.messaging.annotations.MessageHeader;
import com.elderbyte.messaging.annotations.Tombstone;

@Tombstone
public class OrderDeletedMessage extends OrderMessage {

    @MessageHeader
    public String company;

    public OrderDeletedMessage(){}

    public OrderDeletedMessage(String number, String company) {
        this.number = number;
        this.company = company;
    }
}
