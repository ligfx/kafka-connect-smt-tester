package io.github.ligfx;

import static io.github.ligfx.FlexibleSchema.Type.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class FlexibleSchema {
  public enum Type {
    // map directly to Kafka Connect Schema
    INT16,
    INT32,
    INT64,
    FLOAT32,
    FLOAT64,
    BOOLEAN,
    STRING,
    STRUCT,
    ARRAY,
    // new types that don't map cleanly
    NULL,
    RAW_JSON
  };

  private final Type type;
  private FlexibleSchema valueSchema;
  private HashMap<String, FlexibleSchema> objectFields;

  public FlexibleSchema(Type type) {
    this.type = type;
    if (type == Type.STRUCT) {
      this.objectFields = new HashMap<String, FlexibleSchema>();
    }
  }

  public static FlexibleSchema array(FlexibleSchema valueSchema) {
    var schema = new FlexibleSchema(Type.ARRAY);
    schema.valueSchema = valueSchema;
    return schema;
  }

  public Type type() {
    return this.type;
  }

  public void putField(String name, FlexibleSchema schema) {
    this.objectFields.put(name, schema);
  }

  public FlexibleSchema getField(String name) {
    return this.objectFields.getOrDefault(name, new FlexibleSchema(Type.NULL));
  }

  public Set<Map.Entry<String, FlexibleSchema>> fields() {
    return this.objectFields.entrySet();
  }

  public FlexibleSchema valueSchema() {
    return this.valueSchema;
  }

  @Override
  public String toString() {
    return "FlexibleSchema(type=" + this.type.toString() + ")";
  }
}
