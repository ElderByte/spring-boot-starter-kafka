package com.elderbyte.kafka.demo.streams.model;

import java.util.ArrayList;
import java.util.List;

public class OrderUpdated {

    public static final String TOPIC = "demo.store.orders.order";


    public String number;

    public String description;

    public List<OrderItem> items = new ArrayList<>();


    @Override
    public String toString() {
        return "OrderUpdated{" +
                "number='" + number + '\'' +
                ", description='" + description + '\'' +
                ", items=" + items +
                '}';
    }
}
