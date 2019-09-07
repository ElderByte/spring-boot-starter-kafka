package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.messages.MessageBlueprintFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class MessageAnnotationProcessor {


    public static  <K, V> V buildMessage(ConsumerRecord<K, V> record){

        if(record.value() == null) throw new IllegalArgumentException("record must NOT be a tombstone!");
        var value = record.value();
        return buildMessageInt(value, record, value.getClass());
    }

    public static <K, V, D> D buildMessageTombstone(ConsumerRecord<K, V> record, Class<D> tombstoneClazz){
        if(record.value() != null) throw new IllegalArgumentException("record must BE a tombstone!");

        D instance;
        try {
            instance = tombstoneClazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("The given tomb-stone message class could not be instantiated!", e);
        }
        return buildMessageInt(instance, record, tombstoneClazz);
    }

    private static <D, K, V> D buildMessageInt(D value, ConsumerRecord<K, V> record, Class<?> targetClazz){
        var blueprint = MessageBlueprintFactory.lookupOrCreate(targetClazz);
        return blueprint.updateFromRecord(value, record);
    }
}
