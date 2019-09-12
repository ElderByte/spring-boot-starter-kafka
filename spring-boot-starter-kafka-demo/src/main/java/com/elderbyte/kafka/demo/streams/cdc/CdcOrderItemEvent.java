package com.elderbyte.kafka.demo.streams.cdc;

public class CdcOrderItemEvent {

    public static final String TOPIC = "_demo.cdc.orders.item";

    public String orderNumber;

    public String item;

    public int quantity;


    public CdcOrderItemEvent(String orderNumber, String item, int quantity){
        this.orderNumber = orderNumber;
        this.item = item;
        this.quantity = quantity;
    }
}
