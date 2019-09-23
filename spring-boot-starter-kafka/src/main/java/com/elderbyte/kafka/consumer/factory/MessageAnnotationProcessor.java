package com.elderbyte.kafka.consumer.factory;

import com.elderbyte.kafka.messages.MessageBlueprintFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class MessageAnnotationProcessor {


    public static  <K, M> M buildMessage(ConsumerRecord<K, M> record){

        if(record.value() == null) throw new IllegalArgumentException("record must NOT be a tombstone!");
        var value = record.value();
        return buildMessageInt(value, record, (Class<M>)value.getClass());
    }

    public static <K, M, V> M buildMessageTombstone(ConsumerRecord<K, V> record, Class<M> messageClazz){
        if(record.value() != null) throw new IllegalArgumentException("record must BE a tombstone!");

        M instance;
        try {
            instance = messageClazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("The given tomb-stone message class could not be instantiated!", e);
        }
        return buildMessageInt(instance, record, messageClazz);
    }

    private static <K, M, V> M buildMessageInt(M message, ConsumerRecord<K, V> record, Class<M> messageClazz){
        var blueprint = MessageBlueprintFactory.lookupOrCreate(messageClazz);
        return blueprint.updateFromRecord(message, record);
    }
}
