package com.elderbyte.kafka.demo.streams.model;

public class OrderItem {
    public String item;
    public int quantity;

    @Override
    public String toString() {
        return "OrderItem{" +
                "item='" + item + '\'' +
                ", quantity=" + quantity +
                '}';
    }
}
