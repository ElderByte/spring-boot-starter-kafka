package com.elderbyte.kafka.serialisation.key;

import com.elderbyte.kafka.messages.MessageKeyBlueprint;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ElderCompositeKeySerializer<K> implements Serializer<K> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final MessageKeyBlueprint<K> messageKeyBlueprint;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElderCompositeKeySerializer
     */
    public ElderCompositeKeySerializer(Class<K> keyClazz) {
        this(MessageKeyBlueprint.from(keyClazz));
    }

    public ElderCompositeKeySerializer(MessageKeyBlueprint<K> messageKeyBlueprint) {
        this.messageKeyBlueprint = messageKeyBlueprint;
    }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override
    public byte[] serialize(String topic, K data) {
        var serialized = messageKeyBlueprint.serializeKey(data);
        return serialized.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() { }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
