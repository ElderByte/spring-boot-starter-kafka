package com.elderbyte.kafka.serialisation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;


/**
 * Generic JSON deserializer.
 */
public class SpringKafkaJsonDeserializer implements Deserializer<Json> {


  /***************************************************************************
   *                                                                         *
   * Fields                                                                  *
   *                                                                         *
   **************************************************************************/

  private final ObjectMapper objectMapper;

  /***************************************************************************
   *                                                                         *
   * Constructors                                                            *
   *                                                                         *
   **************************************************************************/


  public SpringKafkaJsonDeserializer() {
    this(DefaultJsonMapper.buildDefaultMapper());
  }


  public SpringKafkaJsonDeserializer(ObjectMapper mapper) {
    this.objectMapper = mapper;
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
  public Json deserialize(String ignored, byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return Json.Empty;
    }

    try {
      return Json.from(objectMapper, bytes);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() { }

}