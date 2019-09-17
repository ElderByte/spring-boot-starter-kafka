package com.elderbyte.kafka.demo.streams;

import com.elderbyte.kafka.demo.streams.cdc.CdcEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderItemEvent;
import com.elderbyte.kafka.demo.streams.model.OrderDeletedMessage;
import com.elderbyte.kafka.demo.streams.model.OrderItem;
import com.elderbyte.kafka.demo.streams.model.OrderUpdatedMessage;
import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilder;
import com.elderbyte.kafka.streams.builder.UpdateOrDelete;
import com.elderbyte.kafka.streams.factory.KafkaStreamsContextBuilderFactory;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Set;

@Service
public class OrderUpdatedProducer {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Logger log = LoggerFactory.getLogger(OrderUpdatedProducer.class);

    private final KafkaStreamsContextBuilder builder;
    private final KafkaStreamsContext streamsContext;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    // TODO HeaderEnricher ?

    /**
     * Creates a new OrderUpdatedProducer
     */
    public OrderUpdatedProducer(
            KafkaStreamsContextBuilderFactory streamsBuilderFactory
    ) {



        this.builder = streamsBuilderFactory
                                .newStreamsBuilder("demo");


        orderUpdateKTable()
                .toStream()
                .to("_demo.store.orders.order-header", builder.producedJson(OrderUpdatedMessage.class));

                //.through("_demo.store.orders.order-header", builder.producedJson(OrderUpdatedMessage.class))

        this.builder.tableFromJsonTopic("_demo.store.orders.order-header", OrderUpdatedMessage.class, "order-header")
                .join(
                        orderItemUpdateKTable(),
                        (order, items) -> {
                            if(items != null){
                                order.items = new ArrayList<>(items);
                            }
                            return order;
                        },
                        builder.materializedJson("order-join", OrderUpdatedMessage.class)
                        //builder.joinedJson(OrderUpdatedMessage.class, new TypeReference<Set<OrderItem>>() {})
                )
                .toStream()
                .peek(
                        (key, value) -> {
                            log.info("Peek: " + key + ", value: " + value);
                        }
                ).to(OrderUpdatedMessage.TOPIC, builder.producedJson(OrderUpdatedMessage.class));


        streamsContext = builder.build();
    }



    /***************************************************************************
     *                                                                         *
     * Life Cycle                                                              *
     *                                                                         *
     **************************************************************************/

    @PostConstruct
    public void init(){

        log.info("Starting Topology:\n"+ streamsContext.getTopology().describe());
        // Visualize the topology output @see https://zz85.github.io/kafka-streams-viz/

        streamsContext.start();
    }

    @PreDestroy
    public void destroy(){
        streamsContext.stop();
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/



    private KTable<String, OrderUpdatedMessage> orderUpdateKTable(){

        var cdcOrders = builder.streamFromJsonTopic(CdcOrderEvent.TOPIC, new TypeReference<CdcEvent<CdcOrderEvent>>() {});

        return builder.mapStreamToMessagesTable(
                "orders",
                cdcOrders,
                (k,v) -> {
                    if(!v.delete){
                        return UpdateOrDelete.update(orderUpdated(v.updated));
                    }else{
                        return UpdateOrDelete.delete(orderDeleted(v.updated));
                    }
                },
                OrderUpdatedMessage.class
        );
    }

    private KTable<String, Set<OrderItem>> orderItemUpdateKTable(){

        var cdcOrderItems = builder.streamFromJsonTopic(CdcOrderItemEvent.TOPIC, new TypeReference<CdcEvent<CdcOrderItemEvent>>() {});

        var orderItems = builder.mapStreamToTable(
                "order-items",
                            cdcOrderItems,
                            (k,v) -> {
                                if(!v.delete){
                                    return KeyValue.pair(v.updated.id + "", v.updated);
                                }else{
                                    return KeyValue.pair(v.updated.id + "", null);
                                }
                            },
                            CdcOrderItemEvent.class
                );

        return builder.cdcRecipes()
                .aggregateSet(
                        "order-items-agg",
                        orderItems,
                        (k, v) -> new KeyValue<>(v.orderNumber, itemUpdated(v)),
                        OrderItem.class,
                        new TypeReference<Set<OrderItem>>() {}
                );
    }


    private OrderUpdatedMessage orderUpdated(CdcOrderEvent orderEvent){
        var order = new OrderUpdatedMessage(
                orderEvent.number,
                orderEvent.tenant,
                orderEvent.description
        );
        return order;
    }

    private OrderDeletedMessage orderDeleted(CdcOrderEvent orderEvent){
        var order = new OrderDeletedMessage(
                orderEvent.number,
                orderEvent.tenant
        );
        return order;
    }

    private OrderItem itemUpdated(CdcOrderItemEvent itemEvent){
        var item = new OrderItem();
        item.item = itemEvent.item;
        item.quantity = itemEvent.quantity;
        return item;
    }

}
