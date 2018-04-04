package com.elderbyte.kafka.serialisation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

/**
 * Represents a generic JSON object
 */
public class Json {

    /***************************************************************************
     *                                                                         *
     * Static Builders                                                         *
     *                                                                         *
     **************************************************************************/

    public static Json Empty = new Json(null, null);

    public static Json from(ObjectMapper objectMapper, byte[] data){
        if(objectMapper == null) throw new IllegalArgumentException("objectMapper must not be null!");
        try {
            JsonNode node = objectMapper.readTree(data);
            return new Json(objectMapper, node);
        }catch (Exception e){
            throw new RuntimeException("Failed to deserialize bytes into JSON", e);
        }
    }

    public static Json from(ObjectMapper objectMapper, JsonNode node){
        if(objectMapper == null) throw new IllegalArgumentException("objectMapper must not be null!");
        return new Json(objectMapper, node);
    }

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final ObjectMapper objectMapper;
    private final JsonNode jsonNode;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    private Json(ObjectMapper objectMapper, JsonNode node){
        this.objectMapper = objectMapper;
        this.jsonNode = node;
    }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Get the untyped json node
     */
    public Optional<JsonNode> getJsonNode(){
        return Optional.ofNullable(jsonNode);
    }

    /**
     * Decodes the generic json node into the given Java POJO object
     * @param clazz The target type ()necessary because java
     */
    public <T> Optional<T> json(Class<T> clazz){

        if(clazz == null) throw new IllegalArgumentException("clazz must not be null!");

        if(jsonNode != null){
            try {
                return Optional.of(objectMapper.treeToValue(jsonNode, clazz));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to map json node to class " + clazz.getCanonicalName(), e);
            }
        }else{
            return Optional.empty();
        }
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}