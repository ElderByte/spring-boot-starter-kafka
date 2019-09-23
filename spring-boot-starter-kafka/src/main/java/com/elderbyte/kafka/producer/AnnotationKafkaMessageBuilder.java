package com.elderbyte.kafka.producer;

import com.elderbyte.kafka.messages.InvalidMessageException;
import com.elderbyte.kafka.messages.MessageBlueprintFactory;

public class AnnotationKafkaMessageBuilder {

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Build a kafka message from a generic pojo with message annotations
     * @param message The message object / body
     * @param <M> Type of message object
     * @return Returns a typed kafka message
     */
    public static <K, M> KafkaMessage<K,M> build(M message){
        var blueprint = MessageBlueprintFactory.<K,M>lookupOrCreate((Class<M>)message.getClass());

        var key = blueprint.getKey(message);
        var headers = blueprint.getHeaders(message);

        if(key == null){
            throw new InvalidMessageException("The given message "+message.getClass().getName()+" has null as key value which is not allowed.");
        }

        if (blueprint.isTombstone()){
            return KafkaMessage.tombstone(key, headers);
        }else{
            return KafkaMessage.build(key, message, headers);
        }
    }

}
