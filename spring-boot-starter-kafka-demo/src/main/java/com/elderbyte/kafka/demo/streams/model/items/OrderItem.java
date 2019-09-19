package com.elderbyte.kafka.demo.streams.model.items;

import java.util.Objects;

public class OrderItem {

    public String item;
    public int quantity;

    @Override
    public String toString() {
        return "{" +item + " ("+quantity + ")}";
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderItem orderItem = (OrderItem) o;
        return item.equals(orderItem.item);
    }

    @Override
    public int hashCode() {
        return Objects.hash(item);
    }
}
