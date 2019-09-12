package com.elderbyte.kafka.streams;

import com.elderbyte.kafka.serialisation.ElderKafkaJsonDeserializer;
import com.elderbyte.kafka.serialisation.ElderKafkaJsonSerializer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;

public class ElderJsonSerde<T> extends Serdes.WrapperSerde<T> {

    /***************************************************************************
     *                                                                         *
     * Static Builder                                                          *
     *                                                                         *
     **************************************************************************/

    public static <D> ElderJsonSerde<D> from(ObjectMapper mapper, TypeReference<D> clazz){
        return new ElderJsonSerde<>(
                new ElderKafkaJsonSerializer<>(mapper),
                new ElderKafkaJsonDeserializer<>(clazz, mapper)
        );
    }

    public static <D> ElderJsonSerde<D> from(
            ElderKafkaJsonSerializer<D> serializer,
            ObjectMapper mapper,
            TypeReference<D> clazz)
    {
        return new ElderJsonSerde<>(
                serializer,
                new ElderKafkaJsonDeserializer<>(clazz, mapper)
        );
    }


    public static <D> ElderJsonSerde<D> from(ObjectMapper mapper, Class<D> clazz){
        return new ElderJsonSerde<>(
                new ElderKafkaJsonSerializer<>(mapper),
                new ElderKafkaJsonDeserializer<>(clazz, mapper)
        );
    }

    public static <D> ElderJsonSerde<D> from(
            ElderKafkaJsonSerializer<D> serializer,
            ObjectMapper mapper,
            Class<D> clazz)
    {
        return new ElderJsonSerde<>(
                serializer,
                new ElderKafkaJsonDeserializer<>(clazz, mapper)
        );
    }

    public static <D> ElderJsonSerde<D> from(
            ElderKafkaJsonSerializer<D> serializer,
            ElderKafkaJsonDeserializer<D> deserializer
    ){
        return new ElderJsonSerde<>(
                serializer,
                deserializer
        );
    }

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElderJsonSerde
     */
    private ElderJsonSerde(
            ElderKafkaJsonSerializer<T> serializer,
            ElderKafkaJsonDeserializer<T> deserializer
            ) {
        super(serializer, deserializer);
    }

}
