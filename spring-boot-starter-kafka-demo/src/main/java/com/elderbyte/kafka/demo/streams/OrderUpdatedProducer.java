package com.elderbyte.kafka.demo.streams;

import com.elderbyte.kafka.demo.streams.cdc.CdcEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderItemEvent;
import com.elderbyte.kafka.demo.streams.model.OrderItem;
import com.elderbyte.kafka.demo.streams.model.OrderUpdated;
import com.elderbyte.kafka.streams.ElderJsonSerde;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

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


        // Setup KTable with additional info for order-update
        var orderItemsKTable = orderItemUpdateKStream();

        // Setup Order Update stream
        var orderUpdateKStr = orderUpdateKStream();

        // Join additional info to order update
        orderUpdateKStr.leftJoin(
                orderItemsKTable,
                (order, items) -> {
                    if(items != null){
                        order.items = new ArrayList<>(items);
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

    private KTable<String, Set<OrderItem>> orderItemUpdateKStream(){

        var compactedItemRowsTbl = builder().stream(
                CdcOrderItemEvent.TOPIC,
                Consumed.with(
                        Serdes.String(),
                        ElderJsonSerde.from(mapper, new TypeReference<CdcEvent<CdcOrderItemEvent>>() {}))
        )
        .map((k,v) -> {
            return new KeyValue<>(v.updated.id, v.updated); // TODO Handle deletes
        })
        .groupByKey(serializedJson(Serdes.Long(), CdcOrderItemEvent.class))
        .reduce(
                (old, current) -> current,
                materializedJson("order-item-rows", Serdes.Long(), CdcOrderItemEvent.class)
        );

        return compactedItemRowsTbl.groupBy(
                (k, v) -> new KeyValue<>(v.orderNumber, convert(v)),
                serializedJson(OrderItem.class)
        )
        .aggregate(
                HashSet::new,
                (k,v, agg) -> { agg.add(v); return agg; }, // Adder
                (k,v, agg) -> { agg.remove(v); return agg; }, // Remover
                materializedJson("order-item-aggregated",  new TypeReference<Set<OrderItem>>() {})
        );
    }


    private <K,V> Serialized<K, V> serializedJson(Serde<K> keySerde, Class<V> valueClazz){
        return Serialized.with(keySerde, ElderJsonSerde.from(mapper, valueClazz));
    }

    private <V> Serialized<String, V> serializedJson(Class<V> valueClazz){
        return serializedJson(Serdes.String(), valueClazz);
    }


    private <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, TypeReference<V> clazz){
        return Materialized.<String, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(ElderJsonSerde.from(mapper, clazz));
    }

    private <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Class<V> valueClazz){
        return Materialized.<String, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(ElderJsonSerde.from(mapper, valueClazz));
    }

    private <K,V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedJson(String storeName, Serde<K> keySerde, Class<V> valueClazz){
        return Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(keySerde)
                .withValueSerde(ElderJsonSerde.from(mapper, valueClazz));
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
