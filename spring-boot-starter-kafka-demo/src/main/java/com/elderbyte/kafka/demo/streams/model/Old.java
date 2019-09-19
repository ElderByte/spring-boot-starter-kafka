package com.elderbyte.kafka.demo.streams.model;

public class Old {

    /*
    private void join_stream_tables(){


        orderUpdateKTable()
                .toStream()
                .through("_demo.store.orders.order-header", builder.producedJson(OrderUpdatedMessage.class))
                .leftJoin(
                        orderItemUpdateKTable(),
                        (order, items) -> {
                            if(items != null){
                                order.items = new ArrayList<>(items);
                            }
                            return order;
                        }
                        // builder.materializedJson("order-join", OrderUpdatedMessage.class)
                        // builder.joinedJson(OrderUpdatedMessage.class, new TypeReference<Set<OrderItem>>() {})
                )
                .peek(
                        (key, value) -> {
                            log.info("Peek: " + key + ", value: " + value);
                        }
                ).to(OrderUpdatedMessage.TOPIC, builder.producedJson(OrderUpdatedMessage.class));
    }

    private void join_with_global(){
        final String TOPIC_STORE_ITEMS = "_demo.store.orders.order-items";

        orderItemUpdateKTable()
                .toStream()
                .to(TOPIC_STORE_ITEMS, builder.producedJson(new TypeReference<Set<OrderItem>>() {}));


        var orderItemsLookup = builder.globalTableFromJsonTopic(TOPIC_STORE_ITEMS, new TypeReference<Set<OrderItem>>() {}, "order-items-lookup");


        orderUpdateKTable()
                .toStream()
                .through("_demo.store.orders.order-header", builder.producedJson(OrderUpdatedMessage.class))
                //.to("_demo.store.orders.order-header", builder.producedJson(OrderUpdatedMessage.class));
                .leftJoin(
                        orderItemsLookup,
                        (k, v) -> k,
                        (order, items) -> {
                            if(items != null){
                                order.items = new ArrayList<>(items);
                            }
                            return order;
                        }
                        // builder.materializedJson("order-join", OrderUpdatedMessage.class)
                        // builder.joinedJson(OrderUpdatedMessage.class, new TypeReference<Set<OrderItem>>() {})
                )
                .peek(
                        (key, value) -> {
                            log.info("Peek: " + key + ", value: " + value);
                        }
                ).to(OrderUpdatedMessage.TOPIC, builder.producedJson(OrderUpdatedMessage.class));
    }
     */

}
