package com.elderbyte.kafka.consumer;

public class ProcessingException extends RuntimeException {

    public ProcessingException(String message, Throwable cause){
        super(message, cause);
    }
}
