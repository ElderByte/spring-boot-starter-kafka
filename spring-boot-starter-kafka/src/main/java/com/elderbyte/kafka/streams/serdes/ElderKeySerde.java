package com.elderbyte.kafka.streams.serdes;


import com.elderbyte.kafka.messages.MessageKeyBlueprint;
import com.elderbyte.kafka.serialisation.key.ElderCompositeKeyDeserializer;
import com.elderbyte.kafka.serialisation.key.ElderCompositeKeySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class ElderKeySerde<K> extends Serdes.WrapperSerde<K> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    public static <K> Serde<K> from(Class<K> clazz){

        if(clazz == String.class){
            return (Serde<K>)Serdes.String();
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
