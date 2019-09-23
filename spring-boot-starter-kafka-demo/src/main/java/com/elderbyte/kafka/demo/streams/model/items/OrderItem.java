package com.elderbyte.kafka.demo.streams.model.items;

public class OrderItem {

    public String item;
    public int quantity;

    @Override
    public String toString() {
        return "{" +item + " ("+quantity + ")}";
    }
}
