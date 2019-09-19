package com.elderbyte.kafka.serialisation.json;

/**
 * Exception thrown when the input was not valid JSON
 */
public class JsonParseException extends RuntimeException {

    public JsonParseException(String message, Throwable cause){
        super(message, cause);
    }
    public JsonParseException(String message){
        super(message);
    }
}
