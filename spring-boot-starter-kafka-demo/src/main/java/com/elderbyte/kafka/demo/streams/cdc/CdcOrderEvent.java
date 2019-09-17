package com.elderbyte.kafka.demo.streams.cdc;


public class CdcOrderEvent {

   public static final String TOPIC = "_demo.cdc.orders.order";


   public String number;

   public String tenant;

   public String description;


   public CdcOrderEvent(){}

   public CdcOrderEvent(String number, String tenant, String description){
      this.number = number;
      this.tenant = tenant;
      this.description = description;
   }

   @Override
   public String toString() {
      return "CdcOrderEvent{" +
              "number='" + number + '\'' +
              ", description='" + description + '\'' +
              '}';
   }
}
