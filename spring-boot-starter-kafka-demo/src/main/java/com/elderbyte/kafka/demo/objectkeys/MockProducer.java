package com.elderbyte.kafka.demo.objectkeys;

import com.elderbyte.kafka.demo.streams.model.orders.OrderUpdatedMessage;
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
public class MockProducer {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Logger log = LoggerFactory.getLogger(MockProducer.class);

    public static final String TOPIC = "demo.store.orders.test";

    private final AtomicInteger orderCnt = new AtomicInteger(0);
    private final KafkaMessageProducerTx kafkaProducer;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new CdcMockProducer
     */
    public MockProducer(
            KafkaMessageProducerTx kafkaProducer
    ) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostConstruct
    public void init(){
        sendAllMessages(TOPIC, mockOrderEvents());
    }

    @Scheduled(fixedDelay = 10000)
    public void emitOrderUpdate(){
        sendAllMessages(TOPIC, Arrays.asList(
                mockOrderEvent("A", "test", "A order " + orderCnt.incrementAndGet())
        ));
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private Collection<OrderUpdatedMessage> mockOrderEvents(){
        return Arrays.asList(
                mockOrderEvent("A", "test", "A order"),
                mockOrderEvent("B", "test", "B order"),
                mockOrderEvent("C", "test2", "C order"),
                mockOrderEvent("D", "test2", "D order")
        );
    }

    private OrderUpdatedMessage mockOrderEvent(
            String number,
            String company,
            String description){

        var updated = new OrderUpdatedMessage(number, company, description);
        return updated;
    }


    private void sendAllMessages(String topic, Collection<?> messages){
        log.info("Sending " + messages.size() + " messages to topic " + topic);
        this.kafkaProducer.sendAllTransactionally(topic, (Collection)messages);
    }

}
