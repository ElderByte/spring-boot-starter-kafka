package com.elderbyte.kafka.messages;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.messaging.annotations.MessageMetadata;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;

public class MetadataField {

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
