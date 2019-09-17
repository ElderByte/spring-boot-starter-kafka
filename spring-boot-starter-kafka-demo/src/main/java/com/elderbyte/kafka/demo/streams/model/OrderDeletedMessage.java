package com.elderbyte.kafka.demo.streams.model;

import com.elderbyte.messaging.annotations.MessageKey;
import com.elderbyte.messaging.annotations.MessageMetadata;
import com.elderbyte.messaging.annotations.Tombstone;

@Tombstone
public class OrderDeletedMessage {

    @MessageKey
    public String number;

    @MessageMetadata
    public String company;

    public OrderDeletedMessage(){}

    public OrderDeletedMessage(String number, String company) {
        this.number = number;
        this.company = company;
    }
}
