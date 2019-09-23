package com.elderbyte.kafka.messages;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.messaging.annotations.MessageHeader;
import com.elderbyte.messaging.annotations.MessageKey;
import com.elderbyte.messaging.annotations.Tombstone;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MessageBlueprintFactory {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Map<Class<?>, MessageBlueprint<?,?>> blueprintCache = new ConcurrentHashMap<>();

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    private MessageBlueprintFactory() { }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/


    @SuppressWarnings("unchecked")
    public static <K, M> MessageBlueprint<K,M> lookupOrCreate(Class<M> messageClazz){

        if(messageClazz == null) throw new ArgumentNullException("messageClazz");

        return (MessageBlueprint<K,M>)blueprintCache
                .computeIfAbsent(
                        messageClazz,
                        (clazz) -> fromMessageClass(messageClazz)
                );
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private static <K, M> MessageBlueprint<K,M> fromMessageClass(Class<M> messageClazz) throws InvalidMessageException {

        var isTombstone = messageClazz.getAnnotation(Tombstone.class) != null;

        var metadataFields = new ArrayList<MetadataField>();
        var keyFields = new HashMap<String, MessageKeyField>();

        var fields = messageClazz.getFields();
        for(int i=0; i < fields.length; i++){
            var field = fields[i];
            var messageHeader = field.getAnnotation(MessageHeader.class);

            var messageKey = field.getAnnotation(MessageKey.class);
            if (messageKey != null) {
                keyFields.put(field.getName(), new MessageKeyField(field, messageKey.read()));
            }

            if(isTombstone || messageHeader != null){
                metadataFields.add(
                        MetadataField.from(field, messageHeader)
                );
            }
        }

        if(keyFields.isEmpty()){
            throw new InvalidMessageException("@MessageKey was missing on message class " + messageClazz.getName());
        }else if(keyFields.size() > 1){
            throw new InvalidMessageException("@MessageKey can only be used once on a message class but was used on: " + String.join(",", keyFields.keySet()));
        }
        MessageKeyField keyField = keyFields.values().iterator().next();
        return new MessageBlueprint<>(isTombstone, keyField, metadataFields);
    }


}
