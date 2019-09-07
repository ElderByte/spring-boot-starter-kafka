package com.elderbyte.kafka.messages;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.util.stream.Collectors.toMap;

public class MessageBlueprint {

    private final boolean tombstone;
    private final Field keyField;

    /**
     * Header-Key : Field
     */
    private final Map<String, MetadataField> metadataFields;

    public MessageBlueprint(
            boolean tomstone,
            Field keyField,
            Collection<MetadataField> metadataFields
    ) {

        if(keyField == null) throw new ArgumentNullException("keyField");
        if(metadataFields == null) throw new ArgumentNullException("metadataFields");

        this.tombstone = tomstone;
        this.keyField = keyField;
        this.metadataFields = metadataFields.stream()
                .collect(toMap(MetadataField::getMetadataKey, mf -> mf));
    }


    public boolean isTombstone() {
        return tombstone;
    }

    public <V> String getKey(V message) {
        var value = getFieldAsString(keyField, message);
        if(value == null){
            throw new InvalidMessageException("The key of a message must not be null!");
        }
        return value;
    }

    private <V, K> void setKey(V value, K key) {

    }

    public <V> Map<String, String> getHeaders(V message) {
        var headers = new HashMap<String, String>();
        metadataFields.values().forEach(
                f -> {

                    var val = getFieldAsString(f.getField(), message);
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

    public   <V, K> V updateFromRecord(ConsumerRecord<K, V> record) {
        return updateFromRecord(record.value(), record);
    }

    public  <V, K, M> M updateFromRecord(M message, ConsumerRecord<K, V> record) {

        if(getKey(message) == null){
            setKey(message, record.key());
        }

        var headers = record.headers();

        metadataFields.forEach((k,field) -> {
            var header = headers.lastHeader(k);
            if(header != null){
                setField(field.getField(), message, header.value());
            }
        });

        return message;
    }

    private <V> String getFieldAsString(Field field, V message){
        try {
            var value = field.get(message);
            if(value != null){
                return value.toString();
            }
            return null;
        } catch (IllegalAccessException e) {
            throw new InvalidMessageException("Failed to access value of key field: "+keyField.getName(), e);
        }
    }

    private <V> void setField(Field field, V message, byte[] headerValue){
        setFieldString(field, message, new String(headerValue, StandardCharsets.UTF_8));
    }

    private <V> void setFieldString(Field field, V message, String headerValue){
        try {
            if(field.getType() == String.class){
                field.set(message, headerValue);
            }else{
                throw new InvalidMessageException("Only strings are supported as targets but field" +
                        " "+field.getName()+" was of type "+field.getType()+" ! ");
            }
        } catch (IllegalAccessException e) {
            throw new InvalidMessageException("Failed to access value of key field: "+keyField.getName(), e);
        }
    }





}
