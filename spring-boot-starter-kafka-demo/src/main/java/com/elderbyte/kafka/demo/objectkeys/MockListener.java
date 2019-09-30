package com.elderbyte.kafka.demo.objectkeys;

import com.elderbyte.kafka.consumer.factory.KafkaListenerFactory;
import com.elderbyte.kafka.demo.streams.model.orders.OrderUpdatedMessage;
import com.elderbyte.kafka.metrics.MetricsContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

@Service
public class MockListener {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final Logger log = LoggerFactory.getLogger(MockListener.class);

    private final MessageListenerContainer messageListener;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new CdcMockProducer
     */
    public MockListener(
            KafkaListenerFactory kafkaListenerFactory
    ) {
        this.messageListener = kafkaListenerFactory.start(MockProducer.TOPIC)
                .consumerGroup("kafka-demo")
                .jsonValue(OrderUpdatedMessage.class)
                .stringKey()
                .metrics(MetricsContext.from("kafka-demo", "kafka-demo-1"))
                .blockingRetries(2)
                .buildBatch(this::handle);
    }

    @PostConstruct
    public void init() {
        log.info("Starting kafka listener, listening on topic: " + MockProducer.TOPIC);
        this.messageListener.start();
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private void handle(List<ConsumerRecord<String, OrderUpdatedMessage>> messages) {

        messages.forEach(m ->
            this.log.info("Received message, msg-key: {}, value-key: {}, value: {}",
                    m.key(),
                    m.value().key.toString(),
                    m.value().toString()
            )
        );

    }


}
