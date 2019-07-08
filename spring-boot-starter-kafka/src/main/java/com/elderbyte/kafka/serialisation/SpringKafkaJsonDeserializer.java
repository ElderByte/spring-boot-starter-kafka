package com.elderbyte.kafka.serialisation;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;


/**
 * Generic JSON deserializer.
 */
public class SpringKafkaJsonDeserializer<V> implements Deserializer<V> {


  /***************************************************************************
   *                                                                         *
   * Fields                                                                  *
   *                                                                         *
   **************************************************************************/

  private final StringDeserializer stringDeserializer = new StringDeserializer();
  private final ObjectMapper objectMapper;

  private final Class<V> clazz;
  private final TypeReference<V> typeReference;

  /***************************************************************************
   *                                                                         *
   * Constructors                                                            *
   *                                                                         *
   **************************************************************************/


  public SpringKafkaJsonDeserializer(Class<V> clazz) {
    this(clazz, DefaultJsonMapper.buildDefaultMapper());
  }


  public SpringKafkaJsonDeserializer(Class<V> clazz, ObjectMapper mapper) {
    this(clazz, null, mapper);
  }

  public SpringKafkaJsonDeserializer(TypeReference<V> typeReference, ObjectMapper mapper) {
    this(null, typeReference, mapper);
  }

  private SpringKafkaJsonDeserializer(
          Class<V> clazz,
          TypeReference<V> typeReference,
          ObjectMapper mapper) {
    this.objectMapper = mapper;
    this.typeReference = typeReference;
    this.clazz = clazz;
  }
  /***************************************************************************
   *                                                                         *
   * Public API                                                              *
   *                                                                         *
   **************************************************************************/

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    // NOP
  }


  @Override
  public V deserialize(String topic, byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }

    try {
      if(typeReference != null){
        return objectMapper.readValue(bytes, typeReference);
      }else{
        return objectMapper.readValue(bytes, clazz);
      }
    } catch (Exception e) {
      throw new SerializationException("Failed to deserialize bytes on topic "+topic+" to json: " + stringDeserializer.deserialize(topic, bytes), e);
    }
  }

  @Override
  public void close() { }

}
