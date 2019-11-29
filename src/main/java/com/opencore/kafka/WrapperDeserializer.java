package com.opencore.kafka;

import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

public class WrapperDeserializer implements Deserializer<Object> {
  private Deserializer<Object> wrappedDeserializer = null;

  @Override
  public Object deserialize(String topic, Headers headers, byte[] data) {
    try {
      return wrappedDeserializer.deserialize(topic, headers, data);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.wrappedDeserializer = new WrapperDeserializer();
    wrappedDeserializer.configure(configs, isKey);
  }

  @Override
  public Object deserialize(String s, byte[] bytes) {
    try {
      return wrappedDeserializer.deserialize(s, bytes);
    } catch (Exception e) {
      return null;
    }
  }
}
