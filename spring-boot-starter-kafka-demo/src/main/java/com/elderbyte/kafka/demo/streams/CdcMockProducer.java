package com.elderbyte.kafka.demo.streams;

import com.elderbyte.kafka.demo.streams.cdc.CdcEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderEvent;
import com.elderbyte.kafka.demo.streams.cdc.CdcOrderItemEvent;
import com.elderbyte.kafka.producer.messages.KafkaMessageProducerTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
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

    private static final Logger log = LoggerFactory.getLogger(CdcMockProducer.class);

    private final AtomicInteger orderEventId = new AtomicInteger(0);
    private final AtomicInteger orderItemEventId = new AtomicInteger(0);

    private final AtomicInteger quantityCnt = new AtomicInteger(2);
    private final AtomicInteger orderCnt = new AtomicInteger(0);
    private final KafkaMessageProducerTx messageProducer;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new CdcMockProducer
     */
    public CdcMockProducer(
            KafkaMessageProducerTx messageProducer
    ) {
        this.messageProducer = messageProducer;
    }

    @PostConstruct
    public void init(){
        sendAllMessages(CdcOrderEvent.TOPIC, mockCdcOrderEvents());
        sendAllMessages(CdcOrderItemEvent.TOPIC, mockCdcOrderItemEvents());
    }

    /**/
    @Scheduled(fixedDelay = 5000)
    public void emitItemUpdate(){
        sendAllMessages(CdcOrderItemEvent.TOPIC, Arrays.asList(
                mockCdcOrderItemEvent(1, "A", "item-a-1", quantityCnt.incrementAndGet())
        ));
    }

    @Scheduled(fixedDelay = 10000)
    public void emitOrderUpdate(){
        sendAllMessages(CdcOrderEvent.TOPIC, Arrays.asList(
                mockCdcOrderEvent("A", "A order " + orderCnt.incrementAndGet())
        ));
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
                mockCdcOrderEvent("C", "C order"),
                mockCdcOrderEvent("B", "B order deleted", true),
                mockCdcOrderEvent("D", "D order")
        );
    }

    private Collection<CdcEvent<CdcOrderItemEvent>> mockCdcOrderItemEvents(){
        return Arrays.asList(
                mockCdcOrderItemEvent(1, "A", "ita-1", 1),
                mockCdcOrderItemEvent(2, "A", "ita-2", 10),
                mockCdcOrderItemEvent(3, "A", "ita-3", 20),

                mockCdcOrderItemEvent(1, "A", "ita-1", 2) ,// Update
                mockCdcOrderItemEvent(3, "A", "ita-3 deleted", 0, true), // Delete
                mockCdcOrderItemEvent(2, "A", "ita-2 N", 11, false) // Update
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
                new CdcOrderEvent(number, "holy", description),
                deleted,
                "cdc-order"
        );
    }

    private CdcEvent<CdcOrderItemEvent> mockCdcOrderItemEvent(
            int id,
            String number,
            String item,
            int quantity){
        return mockCdcOrderItemEvent(id, number, item, quantity, false);
    }

    private CdcEvent<CdcOrderItemEvent> mockCdcOrderItemEvent(
            int id,
            String number,
            String item,
            int quantity,
            boolean deleted
    ){
        return new CdcEvent<>(
                orderItemEventId.incrementAndGet(),
                null,
                new CdcOrderItemEvent(id,"holy", number, item, quantity),
                deleted,
                "cdc-item"
        );
    }

    private void sendAllMessages(String topic, Collection<? extends CdcEvent<?>> messages){
        log.info("Sending " + messages.size() + " messages to topic " + topic);
        this.messageProducer.sendAllMessagesTransactionally(topic, messages);
    }

}
