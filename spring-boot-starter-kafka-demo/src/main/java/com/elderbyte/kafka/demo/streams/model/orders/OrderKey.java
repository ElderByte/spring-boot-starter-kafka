package com.elderbyte.kafka.demo.streams.model.orders;

import com.elderbyte.kafka.messages.api.MessageCompositeKey;
import com.elderbyte.messaging.annotations.MessageKey;

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
}
