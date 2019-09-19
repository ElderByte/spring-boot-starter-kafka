package com.elderbyte.kafka.demo.streams;

import com.elderbyte.kafka.demo.streams.cdc.CdcEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderItemEvent;
import com.elderbyte.kafka.demo.streams.model.orders.OrderDeletedMessage;
import com.elderbyte.kafka.demo.streams.model.items.OrderItem;
import com.elderbyte.kafka.demo.streams.model.orders.OrderKey;
import com.elderbyte.kafka.demo.streams.model.orders.OrderUpdatedMessage;
import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilder;
import com.elderbyte.kafka.streams.builder.UpdateOrDelete;
import com.elderbyte.kafka.streams.factory.KafkaStreamsContextBuilderFactory;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.elderbyte.kafka.streams.serdes.ElderKeySerde;
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

        // TODO OffsetOutOfRangeException: Offsets out of range with no configured reset policy for partitions: {_demo.store.orders.order-items-0=2}

        this.builder = streamsBuilderFactory
                                .newStreamsBuilder("demo");


        join_with__table_tables();
        // join_with_global();
        // join_stream_tables();

        streamsContext = builder.build();
    }


    private void join_with__table_tables(){


        orderUpdateKTable()
                .leftJoin(
                        orderItemUpdateKTable(),
                        (order, items) -> {
                            // Support header join behaviour
                            if(items != null){
                                order.items = new ArrayList<>(items);
                            }
                            return order;
                        }
                        // builder.materializedJson("order-join", OrderUpdatedMessage.class)
                        // builder.joinedJson(OrderUpdatedMessage.class, new TypeReference<Set<OrderItem>>() {})
                )
                .toStream()
                .peek(
                        (key, value) -> {
                            log.info("Peek: " + key + ", value: " + value);
                        }
                ).to(OrderUpdatedMessage.TOPIC, builder.producedJson(ElderKeySerde.from(OrderKey.class), OrderUpdatedMessage.class));
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


    private KTable<OrderKey, OrderUpdatedMessage> orderUpdateKTable(){

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
                OrderKey.class,
                OrderUpdatedMessage.class
        );
    }

    private KTable<OrderKey, Set<OrderItem>> orderItemUpdateKTable(){

        var cdcOrderItems = builder.streamFromJsonTopic(CdcOrderItemEvent.TOPIC, new TypeReference<CdcEvent<CdcOrderItemEvent>>() {});

        var orderItems = builder.mapStreamToTable(
                "order-items",
                cdcOrderItems,
                (k,v) -> KeyValue.pair(
                        v.updated.id + "",
                        v.delete ? null : v.updated
                ),
                String.class,
                CdcOrderItemEvent.class
        );

        return builder.cdcRecipes()
                .aggregateSet(
                        "order-items-agg",
                        orderItems,
                        (k, v) -> KeyValue.pair(
                                OrderKey.from(v.tenant, v.orderNumber),
                                itemUpdated(v)
                        ),
                        OrderKey.class,
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
