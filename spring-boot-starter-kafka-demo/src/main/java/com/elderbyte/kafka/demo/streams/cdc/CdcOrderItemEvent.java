package com.elderbyte.kafka.demo.streams.cdc;

public class CdcOrderItemEvent {

    public static final String TOPIC = "_demo.cdc.orders.item";

    public long id;

    public String tenant;

    public String orderNumber;

    public String item;

    public int quantity;

    public CdcOrderItemEvent(){}

    public CdcOrderItemEvent(int id, String tenant, String orderNumber, String item, int quantity){
        this.id = id;
        this.tenant = tenant;
        this.orderNumber = orderNumber;
        this.item = item;
        this.quantity = quantity;
    }
}
