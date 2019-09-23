package com.elderbyte.kafka.demo.streams;

import com.elderbyte.kafka.demo.streams.cdc.CdcEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderItemEvent;
import com.elderbyte.kafka.demo.streams.model.items.OrderItem;
import com.elderbyte.kafka.demo.streams.model.orders.OrderKey;
import com.elderbyte.kafka.demo.streams.model.orders.OrderUpdatedMessage;
import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilder;
import com.elderbyte.kafka.streams.builder.dsl.ElKTable;
import com.elderbyte.kafka.streams.builder.dsl.ElMat;
import com.elderbyte.kafka.streams.factory.KafkaStreamsContextBuilderFactory;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;

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
                .newStreamsBuilder("demo")
                .cleanUpOnError(true); // Recover from state store errors

        join_with__table_tables();

        streamsContext = builder.build();
    }


    private void join_with__table_tables(){


        orderUpdateKTable()
                .joiner()
                    .leftJoin(
                            orderItemUpdateKTable(),
                            (order, items) -> {
                                if(items != null){
                                    order.items = new ArrayList<>(items);
                                }
                                return order;
                            },
                            ElMat.ephemeral()
                    ).toStream()
                    .peek(
                            (key, value) -> {
                                log.info("Peek: " + key + ", value: " + value);
                            }
                    ).to(OrderUpdatedMessage.TOPIC);
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

    private ElKTable<OrderKey, OrderUpdatedMessage> orderUpdateKTable(){

        var cdcOrders = builder.from(String.class, new TypeReference<CdcEvent<CdcOrderEvent>>() {})
                .kstream(CdcOrderEvent.TOPIC);

        return cdcOrders.mapToKey(OrderKey.class)
                    .selectKey((k,v) -> OrderKey.from(v.updated.tenant, v.updated.number))
                    .groupByKey()
                    .latest(
                            (key, value) -> value.delete ? null : orderUpdated(value.updated),
                            ElMat.store("orders"),
                            OrderUpdatedMessage.class
                    );
    }

    private ElKTable<OrderKey, Collection<OrderItem>> orderItemUpdateKTable(){

        var cdcOrderItems = builder.from(String.class, new TypeReference<CdcEvent<CdcOrderItemEvent>>() {})
                .kstream(CdcOrderItemEvent.TOPIC);


        var orderItems = cdcOrderItems
                .mapToKey(String.class)
                    .groupByKey((k,v) -> v.updated.id + "")
                    .latest(
                            (key, value) -> value.delete ? null : value.updated,
                            ElMat.store("order-items"),
                            CdcOrderItemEvent.class
                    );

        return orderItems
                .mapToKey(OrderKey.class)
                    .groupByKey(
                            (k,v) -> OrderKey.from(v.tenant, v.orderNumber)
                    )
                    .aggregateMap( // We should use the key / Map<Key, Value> for the aggregation
                                HashMap::new,
                                (k,v, agg) -> { agg.put(v.id + "", itemUpdated(v)); return agg; }, // Adder
                                (k,v, agg) -> { agg.remove(v.id + ""); return agg; }, // Remover
                                ElMat.store("order-items-agg"), // TODO necessary?
                                new TypeReference<Map<String,OrderItem>>() {}
                        )
                    .mapToValue(new TypeReference<Collection<OrderItem>>() {})
                    .mapValues((k,v) -> v.values(), ElMat.store("items-for-order"));

    }


    private OrderUpdatedMessage orderUpdated(CdcOrderEvent orderEvent){
        var order = new OrderUpdatedMessage(
                orderEvent.number,
                orderEvent.tenant,
                orderEvent.description
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
