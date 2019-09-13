package com.elderbyte.kafka.demo.streams;

import com.elderbyte.kafka.demo.streams.cdc.CdcEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderItemEvent;
import com.elderbyte.kafka.demo.streams.model.OrderItem;
import com.elderbyte.kafka.demo.streams.model.OrderUpdated;
import com.elderbyte.kafka.streams.ElderJsonSerde;
import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilder;
import com.elderbyte.kafka.streams.factory.KafkaStreamsContextBuilderFactory;
import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

@Service
public class OrderUpdatedProducer {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Logger log = LoggerFactory.getLogger(OrderUpdatedProducer.class);

    private final ObjectMapper mapper;
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
            ObjectMapper mapper,
            KafkaStreamsContextBuilderFactory streamsBuilderFactory
    ) {

        this.builder = streamsBuilderFactory
                                .newStreamsBuilder("demo");
        this.mapper = mapper;


        // Setup KTable with additional info for order-update
        var orderItemsKTable = orderItemUpdateKStream();

        // Setup Order Update stream
        var orderUpdateKStr = orderUpdateKStream();

        // Join additional info to order update

        orderUpdateKStr
            .leftJoin(
                orderItemsKTable,
                (order, items) -> {
                    if(items != null){
                        order.items = new ArrayList<>(items);
                    }
                    return order;
                }
            )
            .toStream()
            .peek(
                    (key, value) -> {
                        log.info("Peek: " + key + ", value: " + value);
                    }
            )
            .to(OrderUpdated.TOPIC, Produced.valueSerde(ElderJsonSerde.from(mapper, OrderUpdated.class)));


        streamsContext = builder.build();

        // Visualize the topology output @see https://zz85.github.io/kafka-streams-viz/

    }
    /***************************************************************************
     *                                                                         *
     * Life Cycle                                                              *
     *                                                                         *
     **************************************************************************/

    @PostConstruct
    public void init(){
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

    private KTable<String, OrderUpdated> orderUpdateKStream(){
        return builder.streamsBuilder().stream(
                CdcOrderEvent.TOPIC,
                Consumed.with(
                        Serdes.String(),
                        ElderJsonSerde.from(mapper, new TypeReference<CdcEvent<CdcOrderEvent>>() {}))
        )
        .map((k,v) -> {
            return new KeyValue<>(v.updated.number, convert(v.updated));
        })
        .groupByKey(builder.serializedJson(OrderUpdated.class))
        .reduce(
                (old, current) -> current, // TODO Handle deletes from current.deleted flag
                builder.materializedJson("orders", OrderUpdated.class)
                        .withLoggingDisabled() // Good bad ?
        );
    }

    private KTable<String, Set<OrderItem>> orderItemUpdateKStream(){

        var compactedItemRowsTbl = builder.streamsBuilder().stream(
                CdcOrderItemEvent.TOPIC,
                Consumed.with(
                        Serdes.String(),
                        ElderJsonSerde.from(mapper, new TypeReference<CdcEvent<CdcOrderItemEvent>>() {}))
        )
        .map((k,v) -> {
            return new KeyValue<>(v.updated.id, v.updated);
        })
        .groupByKey(builder.serializedJson(Serdes.Long(), CdcOrderItemEvent.class))
        .reduce(
                (old, current) -> current, // TODO Handle deletes from current.deleted flag
                builder.materializedJson("order-items", Serdes.Long(), CdcOrderItemEvent.class)
                        .withLoggingDisabled() // Good bad ?
        );

        return compactedItemRowsTbl.groupBy(
                (k, v) -> new KeyValue<>(v.orderNumber, convert(v)),
                builder.serializedJson(OrderItem.class)
        )
        .aggregate(
                HashSet::new,
                (k,v, agg) -> { agg.add(v); return agg; }, // Adder
                (k,v, agg) -> { agg.remove(v); return agg; }, // Remover
                builder.materializedJson("order-items-agg",  new TypeReference<Set<OrderItem>>() {})
        );
    }


    private OrderUpdated convert(CdcOrderEvent orderEvent){
        var order = new OrderUpdated();
        order.number = orderEvent.number;
        order.description = orderEvent.description;
        return order;
    }

    private OrderItem convert(CdcOrderItemEvent itemEvent){
        var item = new OrderItem();
        item.item = itemEvent.item;
        item.quantity = itemEvent.quantity;
        return item;
    }

}
