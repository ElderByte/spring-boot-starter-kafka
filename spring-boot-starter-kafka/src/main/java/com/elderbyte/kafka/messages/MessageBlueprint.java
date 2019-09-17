package com.elderbyte.kafka.messages;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.commons.utils.NumberUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.util.stream.Collectors.toMap;

public class MessageBlueprint {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final boolean tombstone;
    private final Field keyField;
    private final boolean populateKeyField;

    /**
     * Header-Key : Field
     */
    private final Map<String, MetadataField> metadataFields;


    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    public MessageBlueprint(
            boolean tomstone,
            Field keyField,
            boolean populateKeyField,
            Collection<MetadataField> metadataFields
    ) {

        if(keyField == null) throw new ArgumentNullException("keyField");
        if(metadataFields == null) throw new ArgumentNullException("metadataFields");

        this.tombstone = tomstone;
        this.keyField = keyField;
        this.populateKeyField = populateKeyField;
        this.metadataFields = metadataFields.stream()
                .collect(toMap(MetadataField::getMetadataKey, mf -> mf));
    }


    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    public boolean isTombstone() {
        return tombstone;
    }


    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/


    public <V> String getKey(V message) {
        var value = getFieldAsString(keyField, message);
        if(value == null){
            throw new InvalidMessageException("The key of a message must not be null!");
        }
        return value;
    }

    public <V> Map<String, String> getHeaders(V message) {
        var headers = new HashMap<String, String>();
        metadataFields.values().forEach(
                f -> {
                    if(f.isWriteToMetadata()){
                        var val = getFieldAsString(f.getField(), message);
                        if(val != null){
                            headers.put(
                                    f.getMetadataKey(),
                                    val
                            );
                        }
                    }
                }
        );
        return headers;
    }

    public <V, K> V updateFromRecord(ConsumerRecord<K, V> record) {
        return updateFromRecord(record.value(), record);
    }

    public <V, K, M> M updateFromRecord(M message, ConsumerRecord<K, V> record) {

        if(populateKeyField){
            if(record.key() != null){
                setFieldString(keyField, message, record.key().toString());
            }
        }

        var headers = record.headers();

        metadataFields.forEach((k,field) -> {
            if(field.isPopulate()){

                if(Map.class.isAssignableFrom(field.getField().getType())){
                    readAllHeadersToMap(headers, message, field.getField());
                }else{
                    // Assume standard field
                    var header = headers.lastHeader(k);
                    if(header != null){
                        setField(field.getField(), message, header.value());
                    }
                }
            }
        });

        return message;
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private void readAllHeadersToMap(Headers headers, Object message, Field mapField){

        var headerMap = new HashMap<String, String>();

        if(headers != null){
            headers.forEach(h -> {
                headerMap.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
            });
        }

        try {
            mapField.set(message, headerMap);
        } catch (IllegalAccessException e) {
            throw new InvalidMessageException("Failed to write value of header map field: "+mapField.getName(), e);
        }
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

    @SuppressWarnings("unchecked")
    private <V> void setFieldString(Field field, V message, String headerValue){
        try {
            if(field.getType() == String.class){
                field.set(message, headerValue);
            }else if(NumberUtil.isNumeric(field.getType())) {
                 var number = NumberUtil.parseNumber(headerValue,  (Class<Number>)field.getType());
                field.set(message, number);
            }else{
                throw new InvalidMessageException("Field " +
                        " "+field.getName()+" was of unsupported type "+field.getType()+" ! ");
            }
        } catch (IllegalAccessException e) {
            throw new InvalidMessageException("Failed to access value of key field: "+keyField.getName(), e);
        }
    }

}
