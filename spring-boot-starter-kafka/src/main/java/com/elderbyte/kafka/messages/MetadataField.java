package com.elderbyte.kafka.messages;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.messaging.annotations.MessageMetadata;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;

public class MetadataField {

    public static MetadataField from(Field field, @Nullable MessageMetadata metadataAttr){

        String metadataKey;
        var writeToMetadata = true;
        var populate = true;

        if(metadataAttr != null && StringUtils.hasText(metadataAttr.key())){
            metadataKey = metadataAttr.key();
            writeToMetadata = metadataAttr.writeToMetadata();
            populate = metadataAttr.populate();
        }else{
            metadataKey = field.getName();
        }

        return new MetadataField(field, metadataKey, writeToMetadata, populate);
    }

    private final Field field;
    private final String metadataKey;
    private final boolean writeToMetadata;
    private final boolean populate;

    public MetadataField(Field field, String metadataKey, boolean writeToMetadata, boolean populate) {

        if(field == null) throw new ArgumentNullException("field");
        if(!StringUtils.hasText(metadataKey)) throw new IllegalArgumentException("metadataKey must have text but was: '"+metadataKey+"'");

        this.field = field;
        this.metadataKey = metadataKey;
        this.writeToMetadata = writeToMetadata;
        this.populate = populate;
    }

    public Field getField() {
        return field;
    }

    public String getMetadataKey() {
        return metadataKey;
    }

    public boolean isWriteToMetadata() {
        return writeToMetadata;
    }

    public boolean isPopulate() {
        return populate;
    }
}
