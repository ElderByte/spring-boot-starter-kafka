package com.elderbyte.kafka.demo.streams.model;

import com.elderbyte.messaging.annotations.MessageKey;
import com.elderbyte.messaging.annotations.MessageMetadata;

import java.util.ArrayList;
import java.util.List;

public class OrderUpdatedMessage {

    public static final String TOPIC = "demo.store.orders.order";

    @MessageKey
    public String number;

    @MessageMetadata
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
