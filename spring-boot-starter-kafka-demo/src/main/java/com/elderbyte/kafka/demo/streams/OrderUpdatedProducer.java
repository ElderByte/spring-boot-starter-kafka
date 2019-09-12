package com.elderbyte.kafka.demo.streams;

import com.elderbyte.kafka.demo.streams.cdc.CdcEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderEvent;
import com.elderbyte.kafka.demo.streams.model.OrderUpdated;
import com.elderbyte.kafka.streams.ElderJsonSerde;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class OrderUpdatedProducer {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Logger log = LoggerFactory.getLogger(OrderUpdatedProducer.class);

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

        // TODO stream.through(Serdes.Integer(), new JsonSerde<>(Cat.class), "cats");

        streamsBuilderFactory.setStateListener((state, old) -> log.info("State: " + state));


        try {
            var builder = streamsBuilderFactory.getObject();

            var stream = builder.stream(
                    CdcOrderEvent.TOPIC,
                    Consumed.with(
                            Serdes.String(),
                            ElderJsonSerde.from(mapper, new TypeReference<CdcEvent<CdcOrderEvent>>() {}))
            ).map((k,v) -> {
                var conv = convert(v.updated);
                return new KeyValue<>(conv.number, conv);
            });

            /*
            .groupBy((k,v) -> v.number).
             */

            stream.peek((key, value) -> {
               log.info("Peek: " + key + ", value: " + value);
            });


        } catch (Exception e) {
            log.error("woot", e);
        }


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


    private OrderUpdated convert(CdcOrderEvent orderEvent){
        var order = new OrderUpdated();
        order.number = orderEvent.number;
        order.description = orderEvent.description;
        return order;
    }

}
