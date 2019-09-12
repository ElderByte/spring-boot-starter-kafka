package com.elderbyte.kafka.demo.streams;

import com.elderbyte.kafka.demo.streams.cdc.CdcEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderItemEvent;
import com.elderbyte.kafka.producer.KafkaProducerTx;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class CdcMockProducer {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private AtomicInteger orderEventId = new AtomicInteger(0);
    private AtomicInteger orderItemEventId = new AtomicInteger(0);


    private final KafkaProducerTx<String, Object> kafkaProducer;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new CdcMockProducer
     */
    public CdcMockProducer(KafkaProducerTx<String, Object> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostConstruct
    public void init(){
        this.kafkaProducer.sendAllMessagesTransactionally(CdcOrderEvent.TOPIC, mockCdcOrderEvents());
        this.kafkaProducer.sendAllMessagesTransactionally(CdcOrderItemEvent.TOPIC, mockCdcOrderItemEvents());
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private Collection<CdcEvent<CdcOrderEvent>> mockCdcOrderEvents(){
        return Arrays.asList(
                mockCdcOrderEvent("A", "A order"),
                mockCdcOrderEvent("B", "B order"),
                mockCdcOrderEvent("C", "C order")
        );
    }

    private Collection<CdcEvent<CdcOrderItemEvent>> mockCdcOrderItemEvents(){
        return Arrays.asList(
                mockCdcOrderItemEvent("A", "item-a-1", 17),
                mockCdcOrderItemEvent("A", "item-a-2", 170),
                mockCdcOrderItemEvent("A", "item-a-3", 170),
                mockCdcOrderItemEvent("A", "item-a-1", 88) // Update
        );
    }

    private CdcEvent<CdcOrderEvent> mockCdcOrderEvent(
            String number,
            String description){
        return mockCdcOrderEvent(number, description, false);
    }
    private CdcEvent<CdcOrderEvent> mockCdcOrderEvent(
            String number,
            String description,
            boolean deleted
    ){
        return new CdcEvent<>(
                orderEventId.incrementAndGet(),
                null,
                new CdcOrderEvent(number, description),
                deleted
        );
    }

    private CdcEvent<CdcOrderItemEvent> mockCdcOrderItemEvent(
            String number,
            String item,
            int quantity){
        return mockCdcOrderItemEvent(number, item, quantity, false);
    }

    private CdcEvent<CdcOrderItemEvent> mockCdcOrderItemEvent(
            String number,
            String item,
            int quantity,
            boolean deleted
    ){
        return new CdcEvent<>(
                orderItemEventId.incrementAndGet(),
                null,
                new CdcOrderItemEvent(number, item, quantity),
                deleted
        );
    }

}
