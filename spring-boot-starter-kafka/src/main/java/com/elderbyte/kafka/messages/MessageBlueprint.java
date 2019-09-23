package com.elderbyte.kafka.messages;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.elderbyte.kafka.messages.ReflectionSupport.setFieldString;
import static java.util.stream.Collectors.toMap;

public class MessageBlueprint<K, M> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final boolean tombstone;
    private final MessageKeyField keyField;

    /**
     * Header-Key : Field
     */
    private final Map<String, MetadataField> headerFields;


    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    public MessageBlueprint(
            boolean tomstone,
            MessageKeyField keyField,
            Collection<MetadataField> headerFields
    ) {

        if(keyField == null) throw new ArgumentNullException("keyField");
        if(headerFields == null) throw new ArgumentNullException("metadataFields");

        this.tombstone = tomstone;
        this.keyField = keyField;
        this.headerFields = headerFields.stream()
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


    public K getKey(M message) {
        var value = ReflectionSupport.getField(keyField.getField(), message);
        return (K)value;
    }

    public Map<String, String> getHeaders(M message) {
        var headers = new HashMap<String, String>();
        headerFields.values().forEach(
                f -> {
                    if(f.isWriteToMetadata()){
                        var val = ReflectionSupport.getFieldAsString(f.getField(), message);
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

    public <RK, RV> M updateFromRecord(M message, ConsumerRecord<RK, RV> record) {


        if(record.key() != null && keyField.isPopulateField()){
            var key = record.key();
            ReflectionSupport.setField(keyField.getField(), message, key);
        }

        var headers = record.headers();

        headerFields.forEach((k, field) -> {
            if(field.isPopulate()){
                if(Map.class.isAssignableFrom(field.getField().getType())){
                    readAllHeadersToMap(headers, message, field.getField());
                }else{
                    // Assume standard field
                    var header = headers.lastHeader(k);
                    if(header != null){
                        setStringFieldBytes(field.getField(), message, header.value());
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

    private <V> void setStringFieldBytes(Field field, V message, byte[] headerValue){
        setFieldString(field, message, new String(headerValue, StandardCharsets.UTF_8));
    }

}
