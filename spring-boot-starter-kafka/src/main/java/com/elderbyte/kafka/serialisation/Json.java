package com.elderbyte.kafka.serialisation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.UnsupportedEncodingException;
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

    public static Json from(ObjectMapper objectMapper, byte[] data){
        if(objectMapper == null) throw new IllegalArgumentException("objectMapper must not be null!");
        return new Json(objectMapper, data);
    }

    public static Json fromDto(ObjectMapper objectMapper, Object pojo){
        if(objectMapper == null) throw new IllegalArgumentException("objectMapper must not be null!");
        if(pojo == null) throw new IllegalArgumentException("pojo must not be null!");

        byte[] utf8Bytes;
        try {
            utf8Bytes = objectMapper.writeValueAsBytes(pojo);
            return new Json(objectMapper, utf8Bytes);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to write json node as bytes", e);
        }
    }

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final ObjectMapper objectMapper;
    private final byte[] jsonData;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    private Json(ObjectMapper objectMapper, byte[] jsonData){
        if(objectMapper == null) throw new IllegalArgumentException("objectMapper must not be null!");
        if(jsonData == null) throw new IllegalArgumentException("node must not be null!");

        this.objectMapper = objectMapper;
        this.jsonData = jsonData;
    }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Get the untyped json node
     * @throws JsonParseException thrown when the data was not in valid json format.
     */
    public JsonNode getJsonNode() throws JsonParseException {
        return parseJson(jsonData);
    }

    public String getContentAsString(){
        try {
            return new String(jsonData,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    /**
     * Decodes the generic json node into the given Java POJO object
     * @param clazz The target type (necessary because java)
     * @throws JsonMappingException thrown when the json cant be mapped to the given dto
     */
    public <T> T json(Class<T> clazz) throws JsonMappingException {

        if(clazz == null) throw new IllegalArgumentException("clazz must not be null!");

        try {
            return objectMapper.treeToValue(getJsonNode(), clazz);
        } catch (JsonProcessingException e) {
            throw new JsonMappingException("Failed to map json node to class " + clazz.getCanonicalName(), e);
        }
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private JsonNode parseJson(byte[] data) throws JsonParseException {

        if(data == null) throw new IllegalArgumentException("data must not be null!");

        try {
            return objectMapper.readTree(data);
        }catch (Exception e){
            throw new JsonParseException("Failed to deserialize bytes into JSON", e); // TODO Proper exception
        }
    }




}