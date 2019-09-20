package com.elderbyte.kafka.demo.streams;

import com.elderbyte.kafka.demo.streams.cdc.CdcEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderItemEvent;
import com.elderbyte.kafka.demo.streams.model.items.OrderItem;
import com.elderbyte.kafka.demo.streams.model.orders.OrderDeletedMessage;
import com.elderbyte.kafka.demo.streams.model.orders.OrderKey;
import com.elderbyte.kafka.demo.streams.model.orders.OrderUpdatedMessage;
import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilder;
import com.elderbyte.kafka.streams.builder.UpdateOrDelete;
import com.elderbyte.kafka.streams.builder.dsl.ElKTable;
import com.elderbyte.kafka.streams.factory.KafkaStreamsContextBuilderFactory;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

@SuppressWarnings("DuplicatedCode")
@Service
public class OrderUpdatedProducerDsl {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Logger log = LoggerFactory.getLogger(OrderUpdatedProducerDsl.class);

    private final KafkaStreamsContextBuilder builder;
    private final KafkaStreamsContext streamsContext;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new OrderUpdatedProducer
     */
    public OrderUpdatedProducerDsl(
            KafkaStreamsContextBuilderFactory streamsBuilderFactory
    ) {

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
                        orderItemUpdateKTable().ktable(),
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
                ).to(OrderUpdatedMessage.TOPIC, builder.serde(OrderKey.class, OrderUpdatedMessage.class).produced());
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

        var cdcOrders = builder.from(String.class, new TypeReference<CdcEvent<CdcOrderEvent>>() {})
                .kstream(CdcOrderEvent.TOPIC);

        return builder.mapStreamToMessagesTable(
                "orders",
                cdcOrders.kstream(),
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

    private ElKTable<OrderKey, Set<OrderItem>> orderItemUpdateKTable(){

        var cdcOrderItems = builder.from(String.class, new TypeReference<CdcEvent<CdcOrderItemEvent>>() {})
                .kstream(CdcOrderItemEvent.TOPIC);


        var orderItems = cdcOrderItems
                .mapToKey(String.class)
                .selectKey((k,v) -> v.updated.id + "")
                .groupByKey()
                .latest(
                        (key, value) -> value.delete ? null : value.updated,
                        "order-items",
                        CdcOrderItemEvent.class
                );

        return orderItems
                .mapTo(OrderKey.class, OrderItem.class)
                    .groupBy(
                            (k,v) -> KeyValue.pair(
                                    OrderKey.from(v.tenant, v.orderNumber),
                                    itemUpdated(v)
                            )
                    ).aggregateMap(
                            HashSet::new,
                            (k,v, agg) -> { agg.add(v); return agg; }, // Adder
                            (k,v, agg) -> { agg.remove(v); return agg; }, // Remover
                            "order-items-agg",
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
