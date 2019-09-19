package com.elderbyte.kafka.demo.streams.model.orders;


import com.elderbyte.kafka.demo.streams.model.items.OrderItem;
import com.elderbyte.messaging.annotations.Message;

import java.util.ArrayList;
import java.util.List;

@Message
public class OrderUpdatedMessage extends OrderMessage {

    public static final String TOPIC = "demo.store.orders.order";

    public String description;
    public List<OrderItem> items = new ArrayList<>();


    public OrderUpdatedMessage(){ }

    public OrderUpdatedMessage(String number, String company, String description) {
        super(number, company);
        this.description = description;
    }

    @Override
    public String toString() {
        return "OrderUpdated{" +
                "number='" + key.number + '\'' +
                ", description='" + description + '\'' +
                ", items=" + items +
                '}';
    }
}
