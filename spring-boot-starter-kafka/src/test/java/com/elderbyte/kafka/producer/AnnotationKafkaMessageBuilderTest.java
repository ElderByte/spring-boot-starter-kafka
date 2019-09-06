package com.elderbyte.kafka.producer;

import com.elderbyte.messaging.annotations.MessageKey;
import com.elderbyte.messaging.annotations.MessageMetadata;
import com.elderbyte.messaging.annotations.Tombstone;
import org.apache.kafka.common.protocol.types.Field;
import org.junit.Test;

import static org.junit.Assert.*;

public class AnnotationKafkaMessageBuilderTest {

    private static class MessageBlob {
        @MessageKey
        public String id;

        @MessageMetadata
        public String test;

        @MessageMetadata(key = "yes.yes")
        public int age;

        public String ignore;


    }

    @Tombstone
    private static class MessageBlobTomb {
        @MessageKey
        public String id;

        @MessageMetadata
        public String test;

        @MessageMetadata(key = "yes.yes")
        public int age;

        public String ignore;

    }


    @Test(expected = IllegalStateException.class)
    public void build_key_null_fail() {
        var obj = new MessageBlob();
        AnnotationKafkaMessageBuilder.build(obj);
    }

    @Test
    public void build_() {
        var obj = new MessageBlob();
        obj.id = "jup";
        var msg = AnnotationKafkaMessageBuilder.build(obj);
        assertEquals("0", msg.getHeaders().get("yes.yes"));
    }


    @Test
    public void build_2() {
        var obj = new MessageBlob();
        obj.id = "jup";
        obj.test = "mock";
        obj.age = 99;
        var msg = AnnotationKafkaMessageBuilder.build(obj);
        assertEquals("mock", msg.getHeaders().get("test"));
        assertEquals("99", msg.getHeaders().get("yes.yes"));
    }


    @Test
    public void build_tomb() {
        var obj = new MessageBlobTomb();
        obj.id = "jup";
        obj.test = "mock";
        obj.age = 99;
        var msg = AnnotationKafkaMessageBuilder.build(obj);
        assertEquals("mock", msg.getHeaders().get("test"));
        assertEquals("99", msg.getHeaders().get("yes.yes"));
        assertNull(msg.getValue());
    }
}
