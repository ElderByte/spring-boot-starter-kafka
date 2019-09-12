package com.elderbyte.kafka.demo.streams.cdc;

public class CdcOrderItemEvent {

    public static final String TOPIC = "_demo.cdc.orders.item";

    public long id;

    public String orderNumber;

    public String item;

    public int quantity;

    public CdcOrderItemEvent(){}

    public CdcOrderItemEvent(int id, String orderNumber, String item, int quantity){
        this.id = id;
        this.orderNumber = orderNumber;
        this.item = item;
        this.quantity = quantity;
    }
}
