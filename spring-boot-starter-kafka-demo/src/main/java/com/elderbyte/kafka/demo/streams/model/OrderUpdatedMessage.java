package com.elderbyte.kafka.demo.streams.model;

import com.elderbyte.messaging.annotations.MessageHeader;
import com.elderbyte.messaging.annotations.MessageKey;

import java.util.ArrayList;
import java.util.List;

public class OrderUpdatedMessage {

    public static final String TOPIC = "demo.store.orders.order";

    @MessageKey
    public String number;

    @MessageHeader
    public String company;

    public String description;

    public List<OrderItem> items = new ArrayList<>();


    public OrderUpdatedMessage(){ }

    public OrderUpdatedMessage(String number, String company, String description) {
        this.number = number;
        this.company = company;
        this.description = description;
    }

    @Override
    public String toString() {
        return "OrderUpdated{" +
                "number='" + number + '\'' +
                ", description='" + description + '\'' +
                ", items=" + items +
                '}';
    }
}
