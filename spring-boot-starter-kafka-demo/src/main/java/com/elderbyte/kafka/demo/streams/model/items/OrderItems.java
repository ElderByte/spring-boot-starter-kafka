package com.elderbyte.kafka.demo.streams.model.items;

import com.elderbyte.kafka.demo.streams.model.orders.OrderKey;
import com.elderbyte.messaging.annotations.MessageKey;
import com.elderbyte.messaging.api.ElderMessage;

import java.util.Set;

public class OrderItems implements ElderMessage<OrderKey> {

    @MessageKey
    public OrderKey key;

    public Set<OrderItem> items;

    public OrderItems(){}
    public OrderItems(OrderKey key, Set<OrderItem> items){
        this.key = key;
        this.items = items;
    }

}
