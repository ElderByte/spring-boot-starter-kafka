package com.elderbyte.kafka.serialisation.key;

import com.elderbyte.commons.utils.NumberUtil;
import com.elderbyte.kafka.messages.MessageKeyBlueprint;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;

public class ElderCompositeKeySerializerGeneric implements Serializer<Object> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private StringSerializer stringSerializer = new StringSerializer();

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElderCompositeKeySerializerGeneric
     */
    public ElderCompositeKeySerializerGeneric() { }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public byte[] serialize(String topic, Object data) {

        if(data == null){
            return new byte[0];
        }

        var dataClazz = data.getClass();

        if(dataClazz == String.class){
            return stringSerializer.serialize(topic, data.toString());
        }else if(NumberUtil.isNumeric(dataClazz)){
            return stringSerializer.serialize(topic, data.toString());
        }else{
            var blueprint = MessageKeyBlueprint.from((Class<Object>)dataClazz);
            var serialized = blueprint.serializeKey(data);
            return serialized.getBytes(StandardCharsets.UTF_8);
        }
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
