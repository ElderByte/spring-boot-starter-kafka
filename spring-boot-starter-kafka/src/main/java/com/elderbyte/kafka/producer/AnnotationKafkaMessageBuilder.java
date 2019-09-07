package com.elderbyte.kafka.producer;

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
     * @param <V> Type of message object
     * @return Returns a typed kafka message
     */
    public static <V> KafkaMessage<String,V> build(V message){
        var blueprint = MessageBlueprintFactory.lookupOrCreate(message.getClass());

        var key = blueprint.getKey(message);
        var headers = blueprint.getHeaders(message);

        if (blueprint.isTombstone()){
            return KafkaMessage.tombstone(key, headers);
        }else{
            return KafkaMessage.build(key, message, headers);
        }
    }

}
