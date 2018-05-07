package com.elderbyte.kafka.serialisation;

/**
 * Exception thrown when the provided json data does not map to the given dto.
 */
public class JsonMappingException extends RuntimeException {

    public JsonMappingException(String message, Throwable cause){
        super(message, cause);
    }
    public JsonMappingException(String message){
        super(message);
    }
}
