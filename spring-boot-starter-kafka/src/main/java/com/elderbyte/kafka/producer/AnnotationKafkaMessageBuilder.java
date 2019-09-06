package com.elderbyte.kafka.producer;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.messaging.annotations.MessageKey;
import com.elderbyte.messaging.annotations.MessageMetadata;
import com.elderbyte.messaging.annotations.Tombstone;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AnnotationKafkaMessageBuilder {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Map<Class<?>, MessageBlueprint> blueprintCache = new ConcurrentHashMap<>();

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
        var blueprint = lookupOrCreate(message.getClass());

        var key = blueprint.getKey(message);
        var headers = blueprint.getHeaders(message);

        if (blueprint.isTombstone()){
            return KafkaMessage.tombstone(key, headers);
        }else{
            return KafkaMessage.build(key, message, headers);
        }
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private static MessageBlueprint lookupOrCreate(Class<?> messageClazz){
        return blueprintCache.computeIfAbsent(messageClazz, AnnotationKafkaMessageBuilder::fromMessageClass);
    }

    private static MessageBlueprint fromMessageClass(Class<?> messageClazz){

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
                    throw new IllegalArgumentException("@MessageKey can only be specified once on a message," +
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
        return new MessageBlueprint(isTombstone, keyField, metadataFields);
    }


    private static class MetadataField {

        public static MetadataField from(Field field, @Nullable MessageMetadata metadataAttr){

            String metadataKey;

            if(metadataAttr != null && StringUtils.hasText(metadataAttr.key())){
                metadataKey = metadataAttr.key();
            }else{
                metadataKey = field.getName();
            }
            return new MetadataField(field, metadataKey);
        }

        private final Field field;
        private final String metadataKey;

        public MetadataField(Field field, String metadataKey) {

            if(field == null) throw new ArgumentNullException("field");
            if(!StringUtils.hasText(metadataKey)) throw new IllegalArgumentException("metadataKey must have text but was: '"+metadataKey+"'");

            this.field = field;
            this.metadataKey = metadataKey;
        }

        public Field getField() {
            return field;
        }

        public String getMetadataKey() {
            return metadataKey;
        }
    }


    private static class MessageBlueprint {

        private final boolean tombstone;
        private final Field keyField;
        private final List<MetadataField> metadataFields;

        public MessageBlueprint(
                boolean tomstone,
                Field keyField,
                Collection<MetadataField> metadataFields
        ) {

            if(keyField == null) throw new ArgumentNullException("keyField");
            if(metadataFields == null) throw new ArgumentNullException("metadataFields");

            this.tombstone = tomstone;
            this.keyField = keyField;
            this.metadataFields = new ArrayList<>(metadataFields);
        }


        public boolean isTombstone() {
            return tombstone;
        }

        public <V> String getKey(V message) {
            var value = getAsString(keyField, message);
            if(value == null){
                throw new IllegalStateException("The key of a message must not be null!");
            }
            return value;
        }

        public <V> Map<String, String> getHeaders(V message) {
            var headers = new HashMap<String, String>();
            metadataFields.forEach(
                    f -> {

                        var val = getAsString(f.field, message);
                        if(val != null){
                            headers.put(
                                    f.getMetadataKey(),
                                    val
                            );
                        }
                    }
            );
            return headers;
        }

        private <V> String getAsString(Field field, V message){
            try {
                var value = field.get(message);
                if(value != null){
                    return value.toString();
                }
                return null;
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Failed to access value of key field: "+keyField.getName(), e);
            }
        }
    }
}
