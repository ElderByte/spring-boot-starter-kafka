package com.elderbyte.kafka.consumer.processing;

/**
 * Exception thrown when the processing failed with an error from which can not be recovered.
 * This happens if a record conversion fails and skipping is disabled.
 */
public class UnrecoverableProcessingException extends ProcessingException {

    public UnrecoverableProcessingException(String message, Throwable cause){
        super(message, cause);
    }
}
