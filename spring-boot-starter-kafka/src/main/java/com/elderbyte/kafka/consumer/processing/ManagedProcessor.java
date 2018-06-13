package com.elderbyte.kafka.consumer.processing;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

public interface ManagedProcessor<K,V> {

    /**
     * Processes the given records. Usually involves decoding the raw records (JSON or other deser)
     *
     * @param rawRecords The raw records
     * @param acknowledgment If this parameter is not null, it means manual ack is required.
     * @param consumer The kafka consumer instance
     */
    void processMessages(List<ConsumerRecord<byte[], byte[]>> rawRecords, Acknowledgment acknowledgment, Consumer<?, ?> consumer);

}
