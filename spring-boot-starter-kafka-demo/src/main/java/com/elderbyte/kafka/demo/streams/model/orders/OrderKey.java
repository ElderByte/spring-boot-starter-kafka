package com.elderbyte.kafka.demo.streams.model.orders;

import com.elderbyte.messaging.annotations.MessageCompositeKey;
import com.elderbyte.messaging.annotations.MessageKey;

import java.util.Objects;

@MessageCompositeKey({"company", "number"})
public class OrderKey {

    public static OrderKey from(String company, String number){
        return new OrderKey(company, number);
    }

    @MessageKey public String number;
    @MessageKey public String company;

    public OrderKey(){}

    public OrderKey(String company, String number) {
        this.company = company;
        this.number = number;
    }


    @Override
    public String toString() {
        return "OrderKey{" +
                "number='" + number + '\'' +
                ", company='" + company + '\'' +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderKey orderKey = (OrderKey) o;
        return Objects.equals(number, orderKey.number) &&
                Objects.equals(company, orderKey.company);
    }

    @Override
    public int hashCode() {
        return Objects.hash(number, company);
    }
}
