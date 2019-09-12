package com.elderbyte.kafka.demo.streams.cdc;

public class CdcOrderEvent {

   public static final String TOPIC = "_demo.cdc.orders.order";


   public String number;

   public String description;


   public CdcOrderEvent(){}

   public CdcOrderEvent(String number, String description){
      this.number = number;
      this.description = description;
   }
}
