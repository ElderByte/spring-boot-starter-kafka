package com.elderbyte.kafka.demo.streams.model;

import com.elderbyte.messaging.annotations.Message;
import com.elderbyte.messaging.annotations.MessageKey;

@Message
public abstract class OrderMessage {
    @MessageKey
    public String number;
}
