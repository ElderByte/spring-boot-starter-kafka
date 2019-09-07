package com.elderbyte.kafka.messages;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.messaging.annotations.MessageKey;
import com.elderbyte.messaging.annotations.MessageMetadata;
import com.elderbyte.messaging.annotations.Tombstone;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageBlueprintFactory {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Map<Class<?>, MessageBlueprint> blueprintCache = new ConcurrentHashMap<>();

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

    public static MessageBlueprint lookupOrCreate(Class<?> messageClazz){
        if(messageClazz == null) throw new ArgumentNullException("messageClazz");
        return blueprintCache.computeIfAbsent(messageClazz, MessageBlueprintFactory::fromMessageClass);
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/


    private static MessageBlueprint fromMessageClass(Class<?> messageClazz) throws InvalidMessageException {

        var isTombstone = messageClazz.getAnnotation(Tombstone.class) != null;

        Field keyField = null;
        var metadataFields = new ArrayList<MetadataField>();

        var fields = messageClazz.getFields();
        for(int i=0; i < fields.length; i++){
            var field = fields[i];
            var messageKey = field.getAnnotation(MessageKey.class);
            var messageMeta = field.getAnnotation(MessageMetadata.class);

            if(messageKey != null){
                if(keyField != null){
                    throw new InvalidMessageException("@MessageKey can only be specified once on a message," +
                            " but was on field " + keyField.getName() + " and on field " + field.getName());
                }
                keyField = field;
            }

            if(isTombstone || messageMeta != null){
                metadataFields.add(
                        MetadataField.from(field, messageMeta)
                );
            }
        }

        if(keyField == null){
            throw new InvalidMessageException("The given class is not a valid message definition since no @MessageKey key is defined!");
        }

        return new MessageBlueprint(isTombstone, keyField, metadataFields);
    }


}
