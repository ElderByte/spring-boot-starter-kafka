package com.elderbyte.kafka.consumer.processing;

public class ProcessingException extends RuntimeException {

    public ProcessingException(String message, Throwable cause){
        super(message, cause);
    }
}
