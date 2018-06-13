package com.elderbyte.kafka.consumer.processing.retry;

import com.elderbyte.kafka.consumer.processing.ProcessingErrorHandler;
import com.elderbyte.kafka.producer.KafkaMessage;
import com.elderbyte.kafka.producer.KafkaProducerTx;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ScheduleRetryErrorHandler<K,V>  implements ProcessingErrorHandler<K,V> {

    private final KafkaProducerTx<String, Object> producer;

    public ScheduleRetryErrorHandler(KafkaProducerTx<String, Object> producer){
        this.producer = producer;
    }

    @Override
    public boolean handleError(List<ConsumerRecord<K, V>> records) {
        var wrapedRetryMessages = warpMessages(records);
        producer.sendAllTransactionally("retry-topic", wrapedRetryMessages);
        return true;
    }

    private Collection<KafkaMessage<String, Object>> warpMessages(List<ConsumerRecord<K, V>> records){
        return new ArrayList<>(); // TODO not yet implemented
    }

}
