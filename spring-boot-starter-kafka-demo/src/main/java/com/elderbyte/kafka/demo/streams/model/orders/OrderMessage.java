package com.elderbyte.kafka.demo.streams.model.orders;

import com.elderbyte.kafka.messages.api.ElderMessage;
import com.elderbyte.messaging.annotations.MessageKey;

public abstract class OrderMessage implements ElderMessage<OrderKey> {

    @MessageKey
    public OrderKey key;

    public OrderMessage(){}

    public OrderMessage(String number, String company) {
        key = OrderKey.from(company, number);
    }
}
