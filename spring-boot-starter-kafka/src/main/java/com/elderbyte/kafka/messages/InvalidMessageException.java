package com.elderbyte.kafka.messages;

public class InvalidMessageException extends RuntimeException {

    public InvalidMessageException(String message){
        super(message);
    }
    public InvalidMessageException(String message, Throwable cause){
        super(message, cause);
    }
}
