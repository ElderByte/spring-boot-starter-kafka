package com.elderbyte.kafka.messages;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.messaging.annotations.Message;
import com.elderbyte.messaging.annotations.MessageKey;
import com.elderbyte.messaging.annotations.MessageHeader;
import com.elderbyte.messaging.annotations.Tombstone;

import java.lang.reflect.Field;
import java.util.*;
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
        var messageAttr = messageClazz.getAnnotation(Message.class);

        var keyFields = new HashMap<String, MessageKeyField>();
        var metadataFields = new ArrayList<MetadataField>();

        var fields = messageClazz.getFields();
        for(int i=0; i < fields.length; i++){
            var field = fields[i];
            var messageKey = field.getAnnotation(MessageKey.class);
            var messageHeader = field.getAnnotation(MessageHeader.class);

            if(messageKey != null){
                keyFields.put(field.getName(), new MessageKeyField(field, messageKey.read()));
            }

            if(isTombstone || messageHeader != null){
                metadataFields.add(
                        MetadataField.from(field, messageHeader)
                );
            }
        }

        if(keyFields.isEmpty()){
            throw new InvalidMessageException("The given class is not a valid message definition since no @MessageKey key is defined!");
        }

        List<MessageKeyField> keyFieldSequence;

        if(messageAttr != null && messageAttr.compositeKey().length > 0){

            // Its a composite key, we have to bring it in order

            if(keyFields.size() != messageAttr.compositeKey().length){
                throw new InvalidMessageException("Each composite-key must have a matching @MessageKey field," +
                        " fields: " + String.join(", ", keyFields.keySet()) +
                        "; composite-keys: " + String.join(", ",  messageAttr.compositeKey())
                );
            }

            keyFieldSequence = new ArrayList<>();

            Arrays.asList(messageAttr.compositeKey())
                    .forEach(compositeKey -> {
                        var field = keyFields.get(compositeKey);
                        if(field != null){
                            keyFieldSequence.add(field);
                        }else{
                            throw new InvalidMessageException("Could not find composite-key field " + compositeKey);
                        }
                    });
        }else{
            // Its a standard key
            if(keyFields.size() > 1){
                throw new InvalidMessageException("@MessageKey can only be specified once on a message without a composite-key," +
                        " but was on fields: " + String.join(", ", keyFields.keySet()));
            }
            keyFieldSequence = Collections.singletonList(keyFields.values().iterator().next());
        }


        return new MessageBlueprint(isTombstone, keyFieldSequence, metadataFields);
    }


}
