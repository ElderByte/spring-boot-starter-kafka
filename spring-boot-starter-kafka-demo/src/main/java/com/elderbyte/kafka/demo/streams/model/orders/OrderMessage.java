package com.elderbyte.kafka.demo.streams.model.orders;

import com.elderbyte.messaging.annotations.MessageKey;
import com.elderbyte.messaging.api.ElderMessage;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

public abstract class OrderMessage implements ElderMessage<OrderKey> {

    @JsonUnwrapped
    @MessageKey public OrderKey key;

    public OrderMessage(){}

    public OrderMessage(String number, String company) {
        key = OrderKey.from(company, number);
    }

}
