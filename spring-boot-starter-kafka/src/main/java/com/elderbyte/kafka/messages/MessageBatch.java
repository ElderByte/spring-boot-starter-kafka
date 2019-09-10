package com.elderbyte.kafka.messages;

import com.elderbyte.kafka.consumer.factory.MessageAnnotationProcessor;
import com.elderbyte.kafka.consumer.processing.Processor;
import com.elderbyte.kafka.records.RecordBatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;

import static java.util.stream.Collectors.toList;


public class MessageBatch<K, M, MT> {

    /***************************************************************************
     *                                                                         *
     * Static Builder                                                          *
     *                                                                         *
     **************************************************************************/

    public static <K, M, MT> MessageBatch<K, M, MT> from(
            Collection<ConsumerRecord<K, M>> records,
            Class<MT> tombstoneClazz){
        return from(RecordBatch.from(records), tombstoneClazz);
    }

    public static <K, M, MT> MessageBatch<K, M, MT> from(
            RecordBatch<K, M> batch,
            Class<MT> tombstoneClazz){
        return new MessageBatch<>(batch, tombstoneClazz);
    }


    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final RecordBatch<K, M> batch;
    private final Class<MT> tombstoneClazz;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new MessagesProcessor
     */
    protected MessageBatch(RecordBatch<K, M> records, Class<MT> tombstoneClazz) {
        this.batch = records;
        this.tombstoneClazz = tombstoneClazz;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Guarantees that the updated / deleted processor are called
     * at most once.
     *
     * Does not guarantee absolute order when invoking the processors.
     * However, does guarantee that only the latest record of a given key
     * is processed at all.
     *
     * Optimal for processing compacted topics.
     */
    public void compactedUpdateOrDelete(
            Processor<List<M>> updatedProcessor,
            Processor<List<MT>> deletedProcessor
    ){
        batch.compactedUpdateOrDelete(
                updated -> updatedProcessor.proccess(updated.stream().map(this::message).collect(toList())),
                deleted -> deletedProcessor.proccess(deleted.stream().map(this::deletedMessage).collect(toList()))
        );
    }


    /**
     * Guarantees absolute order when processing the events.
     *
     * The update / deleted processors bucket messages until the opposite message
     * type is encountered, then the processor is invoked and the bucket of the opposite messages start.
     *
     * Optimizes the number of updated/deleted calls, but always guarantees the absolute message order.
     *
     * Use when replay of events must be in absolute order. Usually non compacted topics.
     */
    public void linearUpdateOrDelete(Processor<List<M>> updatedProcessor,
                                     Processor<List<MT>> deletedProcessor){
        batch.linearUpdateOrDelete(
                updated -> updatedProcessor.proccess(updated.stream().map(this::message).collect(toList())),
                deleted -> deletedProcessor.proccess(deleted.stream().map(this::deletedMessage).collect(toList()))
        );
    }

    @Override
    public String toString() {
        return "MessageBatch{" +
                "batch=" + batch +
                ", tombstoneClazz=" + tombstoneClazz +
                '}';
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private MT deletedMessage(ConsumerRecord<K, M> record){
        return MessageAnnotationProcessor.buildMessageTombstone(record, tombstoneClazz);
    }

    private M message(ConsumerRecord<K, M> record){
        return MessageAnnotationProcessor.buildMessage(record);
    }
}
