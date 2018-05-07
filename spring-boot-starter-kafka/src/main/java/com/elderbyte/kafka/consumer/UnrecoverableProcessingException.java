package com.elderbyte.kafka.consumer;

public class UnrecoverableProcessingException extends ProcessingException{

    public UnrecoverableProcessingException(String message, Throwable cause){
        super(message, cause);
    }
}
