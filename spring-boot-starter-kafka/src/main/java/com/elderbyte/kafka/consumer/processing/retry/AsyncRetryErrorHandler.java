package com.elderbyte.kafka.consumer.processing.retry;

import com.elderbyte.kafka.consumer.processing.ProcessingErrorHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public class AsyncRetryErrorHandler<K,V>  implements ProcessingErrorHandler<K,V> {

    @Override
    public boolean handleError(List<ConsumerRecord<K, V>> records) {
        return false;
    }
}
