package com.elderbyte.kafka.demo.streams;

import com.elderbyte.kafka.demo.streams.cdc.CdcEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderItemEvent;
import com.elderbyte.kafka.demo.streams.model.OrderItem;
import com.elderbyte.kafka.demo.streams.model.OrderUpdated;
import com.elderbyte.kafka.streams.ElderJsonSerde;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@Service
public class OrderUpdatedProducer {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Logger log = LoggerFactory.getLogger(OrderUpdatedProducer.class);

    private final ObjectMapper mapper;
    private final StreamsBuilderFactoryBean streamsBuilderFactory;

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
            StreamsBuilderFactoryBean streamsBuilderFactory
    ) {

        this.mapper = mapper;
        this.streamsBuilderFactory = streamsBuilderFactory;

        streamsBuilderFactory.setStateListener((state, old) -> log.info("State: " + state));

        var orderUpdateKStr = orderUpdateKStream();

        /*
        .groupBy((k,v) -> v.number).
         */

        /*
        orderUpdateKStr.peek((key, value) -> {
            log.info("Peek: " + key + ", value: " + value);
        });*/

        var orderItemsKTable = orderItemUpdateKStream();


        orderUpdateKStr.leftJoin(
                orderItemsKTable,
                (order, items) -> {
                    if(items != null){
                        order.items = items;
                    }
                    return order;

                },
                Joined.valueSerde(ElderJsonSerde.from(mapper, OrderUpdated.class))
        )
                .peek(
                        (key, value) -> {
                            log.info("Peek: " + key + ", value: " + value);
                        }
                )
                .to(OrderUpdated.TOPIC, Produced.valueSerde(ElderJsonSerde.from(mapper, OrderUpdated.class)));


    }
    /***************************************************************************
     *                                                                         *
     * Life Cycle                                                              *
     *                                                                         *
     **************************************************************************/

    @PostConstruct
    public void init(){

    }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private KStream<String, OrderUpdated> orderUpdateKStream(){
        return builder().stream(
                CdcOrderEvent.TOPIC,
                Consumed.with(
                        Serdes.String(),
                        ElderJsonSerde.from(mapper, new TypeReference<CdcEvent<CdcOrderEvent>>() {}))
        ).map((k,v) -> {
            var conv = convert(v.updated);
            return new KeyValue<>(conv.number, conv);
        });
    }

    private KTable<String, List<OrderItem>> orderItemUpdateKStream(){
        return builder().stream(
                CdcOrderItemEvent.TOPIC,
                Consumed.with(
                        Serdes.String(),
                        ElderJsonSerde.from(mapper, new TypeReference<CdcEvent<CdcOrderItemEvent>>() {}))
        ).map((k,v) -> {
            var conv = convert(v.updated);
            return new KeyValue<>(v.updated.orderNumber, conv);
        }).groupByKey(Serialized.with(Serdes.String(), ElderJsonSerde.from(mapper, OrderItem.class)))
            .aggregate(
                    ArrayList::new,
                    (k,v, agg) -> { agg.add(v); return agg; },
                    Materialized.with(Serdes.String(), ElderJsonSerde.from(mapper, new TypeReference<List<OrderItem>>() {}))
            );
    }

    private StreamsBuilder builder(){
        try {
            return streamsBuilderFactory.getObject();
        } catch (Exception e) {
            throw new RuntimeException("", e);
        }
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
