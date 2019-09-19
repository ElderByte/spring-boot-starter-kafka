package com.elderbyte.kafka.demo.streams.model.orders;

import com.elderbyte.messaging.annotations.Message;
import com.elderbyte.messaging.annotations.MessageKey;

@Message(compositeKey = {"company", "number"})
public abstract class OrderMessage {

    @MessageKey
    public String number;

    @MessageKey
    public String company;


    public OrderMessage(){}


    public OrderMessage(String number, String company) {
        this.number = number;
        this.company = company;
    }
}
