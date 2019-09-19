package com.elderbyte.kafka.messages.api;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Inherited
public @interface MessageCompositeKey {

    /**
     * Denotes a composite message key.
     * Each value must represent a field in the message body.
     */
    String[] value();
}
