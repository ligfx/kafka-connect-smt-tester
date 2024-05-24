package io.github.ligfx;

import static io.github.ligfx.FlexibleSchema.Type.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

public class EasyJsonConverter implements Converter {

  public EasyJsonConverter() {}

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    throw new DataException("Not implemented");
  }

  private <T> Iterable<T> asIterable(Iterator<T> iter) {
    return new Iterable<T>() {
      public Iterator<T> iterator() {
        return iter;
      }
    };
  }

  private boolean matchSchemaTypes(
      FlexibleSchema left, FlexibleSchema right, FlexibleSchema.Type... types) {
    boolean matchedLeft = false;
    boolean matchedRight = false;
    for (var t : types) {
      if (left.type() == t) {
        matchedLeft = true;
      }
      if (right.type() == t) {
        matchedRight = true;
      }
      if (matchedLeft && matchedRight) {
        return true;
      }
    }
    return false;
  }

  private FlexibleSchema combineSchemas(FlexibleSchema left, FlexibleSchema right) {
    if (left.type() == NULL) {
      return right;
    } else if (right.type() == NULL) {
      return left;
    } else if (matchSchemaTypes(left, right, BOOLEAN)) {
      return new FlexibleSchema(BOOLEAN);
    } else if (matchSchemaTypes(left, right, STRING)) {
      return new FlexibleSchema(STRING);
    } else if (matchSchemaTypes(left, right, INT16)) {
      return new FlexibleSchema(INT16);
    } else if (matchSchemaTypes(left, right, INT16, INT32)) {
      return new FlexibleSchema(INT32);
    } else if (matchSchemaTypes(left, right, INT16, INT32, INT64)) {
      return new FlexibleSchema(INT64);
    } else if (matchSchemaTypes(left, right, INT16, INT32, INT64, FLOAT32)) {
      return new FlexibleSchema(FLOAT32);
    } else if (matchSchemaTypes(left, right, INT16, INT32, INT64, FLOAT32, FLOAT64)) {
      return new FlexibleSchema(FLOAT64);

    } else if (matchSchemaTypes(left, right, ARRAY)) {
      // coerce the internal types
      return FlexibleSchema.array(combineSchemas(left.valueSchema(), right.valueSchema()));

    } else if (matchSchemaTypes(left, right, STRUCT)) {
      // coerce each field type
      var combinedSchema = new FlexibleSchema(STRUCT);
      for (var entry : left.fields()) {
        combinedSchema.putField(entry.getKey(), entry.getValue());
      }
      for (var entry : right.fields()) {
        combinedSchema.putField(
            entry.getKey(),
            combineSchemas(left.getField(entry.getKey()), right.getField(entry.getKey())));
      }
      return combinedSchema;

    } else {
      // Couldn't combine them cleanly, just serialize each element as a string of raw JSON
      return new FlexibleSchema(RAW_JSON);
    }
  }

  private FlexibleSchema buildArraySchema(List<FlexibleSchema> schemas) {
    if (schemas.size() == 0) {
      return new FlexibleSchema(FlexibleSchema.Type.NULL);
    }
    FlexibleSchema combinedSchema = new FlexibleSchema(FlexibleSchema.Type.NULL);
    for (var nextSchema : schemas) {
      combinedSchema = combineSchemas(combinedSchema, nextSchema);
    }
    return FlexibleSchema.array(combinedSchema);
  }

  private Schema schemaForFlexibleSchema(FlexibleSchema schema) {
    if (schema.type() == FlexibleSchema.Type.NULL
        || schema.type() == FlexibleSchema.Type.RAW_JSON) {
      // TODO: add param to tell downstream this was raw json
      return new SchemaBuilder(Schema.Type.STRING).optional().parameter("raw_json", "true").build();

    } else if (schema.type() == FlexibleSchema.Type.STRUCT) {
      var builder = SchemaBuilder.struct().optional();
      for (var entry : schema.fields()) {
        builder.field(entry.getKey(), schemaForFlexibleSchema(entry.getValue()));
      }
      return builder.build();

    } else if (schema.type() == FlexibleSchema.Type.ARRAY) {
      return SchemaBuilder.array(schemaForFlexibleSchema(schema.valueSchema())).optional().build();

    } else if (schema.type() == INT32) {
      return Schema.OPTIONAL_INT32_SCHEMA;
    } else if (schema.type() == INT64) {
      return Schema.OPTIONAL_INT64_SCHEMA;
    } else if (schema.type() == FLOAT32) {
      return Schema.OPTIONAL_FLOAT32_SCHEMA;
    } else if (schema.type() == FLOAT64) {
      return Schema.OPTIONAL_FLOAT64_SCHEMA;
    } else if (schema.type() == BOOLEAN) {
      return Schema.OPTIONAL_BOOLEAN_SCHEMA;
    } else if (schema.type() == STRING) {
      return Schema.OPTIONAL_STRING_SCHEMA;
    }
    throw new DataException("Not implemented: " + schema.toString());
  }

  private Object valueForNode(JsonNode node, FlexibleSchema schema) {
    if (schema.type() == FlexibleSchema.Type.NULL
        || schema.type() == FlexibleSchema.Type.RAW_JSON) {
      var mapper = new ObjectMapper(); // TODO: create once, reuse
      try {
        return mapper.writeValueAsString(node);
      } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
        throw new DataException("error serializing node", e);
      }

    } else if (node.isNull()) {
      return null;

    } else if (schema.type() == FlexibleSchema.Type.STRUCT) {
      var kafkaSchema = schemaForFlexibleSchema(schema);
      var struct = new Struct(kafkaSchema);
      for (var entry : schema.fields()) {
        var child = node.get(entry.getKey());
        if (child == null) {
          continue;
        }
        struct.put(entry.getKey(), valueForNode(child, entry.getValue()));
      }
      return struct;

    } else if (schema.type() == FlexibleSchema.Type.ARRAY) {
      var array = new ArrayList<Object>();
      for (var child : node) {
        array.add(valueForNode(child, schema.valueSchema()));
      }
      return array;

    } else if (schema.type() == FlexibleSchema.Type.STRING) {
      return node.textValue();
    } else if (schema.type() == FlexibleSchema.Type.INT16) {
      return node.shortValue();
    } else if (schema.type() == FlexibleSchema.Type.INT32) {
      return node.intValue();
    } else if (schema.type() == FlexibleSchema.Type.INT64) {
      return node.longValue();
    } else if (schema.type() == FlexibleSchema.Type.FLOAT32) {
      return node.floatValue();
    } else if (schema.type() == FlexibleSchema.Type.FLOAT64) {
      return node.doubleValue();
    } else if (schema.type() == FlexibleSchema.Type.BOOLEAN) {
      return node.booleanValue();
    }
    throw new DataException("Not implemented: " + schema.toString() + " " + node.toString());
  }

  private FlexibleSchema flexibleSchemaForNode(JsonNode node) {
    // TODO: could probably do this more efficently using Jackson's streaming API
    if (node.isObject()) {
      var schema = new FlexibleSchema(FlexibleSchema.Type.STRUCT);
      for (var entry : asIterable(node.fields())) {
        if (entry.getValue().isNull()) {
          // just drop null values, can't create a schema for them
          continue;
        }
        var childSchema = flexibleSchemaForNode(entry.getValue());
        if (childSchema.type() == NULL) {
          // drop null schemas too (usually an empty array?)
          continue;
        }
        schema.putField(entry.getKey(), flexibleSchemaForNode(entry.getValue()));
      }
      return schema;

    } else if (node.isArray()) {
      var children = new ArrayList<FlexibleSchema>();
      for (var child : node) {
        children.add(flexibleSchemaForNode(child));
      }
      return buildArraySchema(children);

    } else if (node.isTextual()) {
      return new FlexibleSchema(FlexibleSchema.Type.STRING);
    } else if (node.isShort()) {
      // in practice this doesn't seem to get triggered, just use INT32?
      return new FlexibleSchema(FlexibleSchema.Type.INT16);
    } else if (node.isInt()) {
      // TODO: just use INT64 for everything?
      return new FlexibleSchema(FlexibleSchema.Type.INT32);
    } else if (node.isLong()) {
      return new FlexibleSchema(FlexibleSchema.Type.INT64);
    } else if (node.isFloat()) {
      // TODO: in practice this doesn't seem to get triggered, just use FLOAT64?
      return new FlexibleSchema(FlexibleSchema.Type.FLOAT32);
    } else if (node.isDouble()) {
      return new FlexibleSchema(FlexibleSchema.Type.FLOAT64);
    } else if (node.isBoolean()) {
      return new FlexibleSchema(FlexibleSchema.Type.BOOLEAN);
    } else if (node.isNull()) {
      return new FlexibleSchema(FlexibleSchema.Type.NULL);
    }
    throw new DataException("Error getting schema and value for node: " + node.toString());
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    var mapper = new ObjectMapper(); // TODO: create once, reuse
    try {
      var root = mapper.readTree(value);
      var flexibleSchema = flexibleSchemaForNode(root);
      return new SchemaAndValue(
          schemaForFlexibleSchema(flexibleSchema), valueForNode(root, flexibleSchema));
    } catch (java.io.IOException e) {
      throw new DataException("Failed to deserialize: ", e);
    }
  }
}
