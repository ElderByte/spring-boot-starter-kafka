package com.elderbyte.kafka.messages;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.commons.utils.NumberUtil;
import com.elderbyte.messaging.MessageKeyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class MessageBlueprint {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final boolean tombstone;
    private final List<MessageKeyField> keyFields;

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
            List<MessageKeyField> keyFields,
            Collection<MetadataField> headerFields
    ) {

        if(keyFields == null) throw new ArgumentNullException("keyFields");
        if(headerFields == null) throw new ArgumentNullException("metadataFields");

        this.tombstone = tomstone;
        this.keyFields = keyFields;
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


    public <V> String getKey(V message) {

        String value;

        if(keyFields.size() > 1){
            value = MessageKeyUtil.compositeKey(
                    keyFields.stream()
                            .map(kf -> getRequiredFieldAsString(kf.getField(), message))
                            .collect(toList())
            );
        }else{
            var keyField = keyFields.get(0);
            value = getRequiredFieldAsString(keyField.getField(), message);
        }
        return value;
    }

    public <V> Map<String, String> getHeaders(V message) {
        var headers = new HashMap<String, String>();
        headerFields.values().forEach(
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


        if(keyFields.size() == 1){
            var field = keyFields.get(0);
            if(field.isPopulateField()){
                if(record.key() != null){
                    setFieldString(field.getField(), message, record.key().toString());
                }
            }
        }else{
            var key = record.key().toString();
            var values = MessageKeyUtil.parseCompositeKey(key);

            for(int i = 0;i < keyFields.size(); i++){
                var k = keyFields.get(i);
                if(k.isPopulateField()){
                    var v = values.get(i);
                    setFieldString(k.getField(), message, v);
                }
            }
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
            throw new InvalidMessageException("Failed to access value of field: "+field.getName(), e);
        }
    }

    private <V> String getRequiredFieldAsString(Field field, V message){
        var val = getFieldAsString(field, message);
        if(val == null){
            throw new InvalidMessageException("The field "+field.getName()+" of the message must not be null!");
        }
        return val;
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
            throw new InvalidMessageException("Failed to access value of key field: "+field.getName(), e);
        }
    }

}
