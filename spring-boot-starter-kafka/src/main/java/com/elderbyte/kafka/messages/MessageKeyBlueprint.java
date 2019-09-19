package com.elderbyte.kafka.messages;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.messaging.MessageKeyUtil;
import com.elderbyte.messaging.annotations.MessageCompositeKey;
import com.elderbyte.messaging.annotations.MessageKey;

import java.lang.reflect.Constructor;
import java.util.*;

import static java.util.stream.Collectors.toList;

public class MessageKeyBlueprint<K> {

    /***************************************************************************
     *                                                                         *
     * Static Builder                                                          *
     *                                                                         *
     **************************************************************************/

    public static <K> MessageKeyBlueprint<K> from(Class<K> keyClazz){

        var compositeKey = keyClazz.getAnnotation(MessageCompositeKey.class);
        var keyFields = new HashMap<String, MessageKeyField>();

        var fields = keyClazz.getFields();
        for (java.lang.reflect.Field field : fields) {
            var messageKey = field.getAnnotation(MessageKey.class);
            if (messageKey != null) {
                keyFields.put(field.getName(), new MessageKeyField(field, messageKey.read()));
            }
        }

        if(keyFields.isEmpty()){
            throw new InvalidMessageException("The given class "+keyClazz.getName()+" is not a valid message-key since no @MessageKey key is defined!");
        }

        List<MessageKeyField> keyFieldSequence;

        if(compositeKey != null && compositeKey.value().length > 0){

            var compositeFields = compositeKey.value();

            // Its a composite key, we have to bring it in order

            if(keyFields.size() != compositeFields.length){
                throw new InvalidMessageException("Each composite-key must have a matching @MessageKey field," +
                        " fields: " + String.join(", ", keyFields.keySet()) +
                        "; composite-keys: " + String.join(", ",  compositeFields)
                );
            }

            keyFieldSequence = new ArrayList<>();

            Arrays.asList(compositeFields)
                    .forEach(k -> {
                        var field = keyFields.get(k);
                        if(field != null){
                            keyFieldSequence.add(field);
                        }else{
                            throw new InvalidMessageException("Could not find composite-key field " + k);
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

        return new MessageKeyBlueprint<>(keyClazz, keyFieldSequence);
    }

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final Class<K> keyClazz;
    private final List<MessageKeyField> keyFields;
    private final Constructor<K> keyConstructor;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    private MessageKeyBlueprint(
            Class<K> keyClazz,
            List<MessageKeyField> keyFields
    ){
        if(keyClazz == null) throw new ArgumentNullException("keyClazz");
        if(keyFields == null) throw new ArgumentNullException("keyFields");
        this.keyClazz = keyClazz;
        this.keyFields = keyFields;

        try {
            this.keyConstructor = keyClazz.getDeclaredConstructor();
        } catch (Exception e) {
            throw new InvalidMessageException("Failed to locate no args constructor for message-key object "+keyClazz.getName(), e);
        }
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Deserialize the given composite-string-key and write to the given object fields.
     */
    public void deserializeKeyTo(String serializedKey, K message){
        if(keyFields.size() == 1){
            var field = keyFields.get(0);
            if(field.isPopulateField()){
                if(serializedKey != null){
                    ReflectionSupport.setFieldString(field.getField(), message, serializedKey);
                }
            }
        }else{
            var values = MessageKeyUtil.parseCompositeKey(serializedKey);
            for(int i = 0;i < keyFields.size(); i++){
                var k = keyFields.get(i);
                if(k.isPopulateField()){
                    var v = values.get(i);
                    ReflectionSupport.setFieldString(k.getField(), message, v);
                }
            }
        }
    }

    public String serializeKey(K message) {
        String value;
        if(keyFields.size() > 1){
            value = MessageKeyUtil.compositeKey(
                    keyFields.stream()
                            .map(kf -> ReflectionSupport.getRequiredFieldAsString(kf.getField(), message))
                            .collect(toList())
            );
        }else{
            var keyField = keyFields.get(0);
            value = ReflectionSupport.getRequiredFieldAsString(keyField.getField(), message);
        }
        return value;
    }

    public K deserializeKey(String serializedStr) {
        K newKey;
        try {
            newKey = keyConstructor.newInstance();
        } catch (Exception e) {
            throw new InvalidMessageException("Failed to instantiate message-key object "+keyClazz.getName()+" when deserializing key " + serializedStr, e);
        }
        deserializeKeyTo(serializedStr, newKey);
        return newKey;
    }


    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
