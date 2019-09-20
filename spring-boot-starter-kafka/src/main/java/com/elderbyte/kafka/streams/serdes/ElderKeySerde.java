package com.elderbyte.kafka.streams.serdes;


import com.elderbyte.commons.exceptions.ArgumentNullException;
import com.elderbyte.commons.utils.NumberUtil;
import com.elderbyte.kafka.messages.MessageKeyBlueprint;
import com.elderbyte.kafka.serialisation.key.ElderCompositeKeyDeserializer;
import com.elderbyte.kafka.serialisation.key.ElderCompositeKeySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ElderKeySerde<K> extends Serdes.WrapperSerde<K> {

    /***************************************************************************
     *                                                                         *
     * Static Fields                                                           *
     *                                                                         *
     **************************************************************************/

    private static final Map<Class<?>, Serde<?>> serdeCache = new ConcurrentHashMap<>();

    /***************************************************************************
     *                                                                         *
     * Static Builder                                                          *
     *                                                                         *
     **************************************************************************/

    @SuppressWarnings("unchecked")
    public static <K> Serde<K> from(Class<K> clazz){

        if(clazz == null) throw new ArgumentNullException("clazz");

        if(clazz == String.class){
            return (Serde<K>)Serdes.String();
        }

        return (Serde<K>)serdeCache
                .computeIfAbsent(
                        clazz,
                        ElderKeySerde::buildSerde
                );
    }

    /***************************************************************************
     *                                                                         *
     * Private                                                                 *
     *                                                                         *
     **************************************************************************/


    private static <K> ElderKeySerde<K> buildSerde(Class<K> clazz){

        if(NumberUtil.isNumeric(clazz))  {
            throw new IllegalArgumentException("Only classes with @MessageCompositeKey are supported.");
        }

        var keyBlueprint = MessageKeyBlueprint.from(clazz);
        var serializer = new ElderCompositeKeySerializer<>(keyBlueprint);
        var deserializer = new ElderCompositeKeyDeserializer<>(keyBlueprint);
        return new ElderKeySerde<>(serializer, deserializer);
    }

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElderJsonSerde
     */
    private ElderKeySerde(
            ElderCompositeKeySerializer<K> serializer,
            ElderCompositeKeyDeserializer<K> deserializer
    ) {
        super(serializer, deserializer);
    }

}
