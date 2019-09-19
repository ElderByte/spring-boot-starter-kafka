package com.elderbyte.kafka.serialisation.key;

import com.elderbyte.kafka.messages.MessageKeyBlueprint;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ElderCompositeKeyDeserializer<K> implements Deserializer<K> {

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
    public ElderCompositeKeyDeserializer(Class<K> keyClazz) {
        this(MessageKeyBlueprint.from(keyClazz));
    }

    public ElderCompositeKeyDeserializer(MessageKeyBlueprint<K> messageKeyBlueprint) {
        this.messageKeyBlueprint = messageKeyBlueprint;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override
    public K deserialize(String topic, byte[] data) {
        var serializedStr = new String(data, StandardCharsets.UTF_8);
        return messageKeyBlueprint.deserializeKey(serializedStr);
    }

    @Override
    public void close() { }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
