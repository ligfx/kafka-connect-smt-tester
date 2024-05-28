package io.github.ligfx;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public class LoggingSinkTask extends SinkTask {
  @Override
  public String version() {
    return new LoggingSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {}

  private String schemaToStringDetailed(Schema schema) {
    var builder = new StringBuilder();
    if (schema.name() != null) {
      builder.append(schema.name());
      builder.append(":");
    }
    builder.append(schema.type().getName().toUpperCase());
    if (schema.isOptional()) {
      builder.append("?");
    }

    if (schema.parameters() != null && schema.parameters().size() > 0) {
      boolean first = true;
      for (var entry : schema.parameters().entrySet()) {
        if (first) {
          builder.append("!");
          first = false;
        } else {
          builder.append(",");
        }
        builder.append(entry.getKey().toString());
        builder.append("=");
        builder.append(entry.getValue().toString());
      }
    }

    if (schema.type() == Schema.Type.ARRAY) {
      builder.append("{");
      builder.append(schemaToStringDetailed(schema.valueSchema()));
      builder.append("}");

    } else if (schema.type() == Schema.Type.STRUCT) {
      builder.append("{");
      for (var field : schema.fields()) {
        if (field.index() != 0) {
          builder.append(", ");
        }
        builder.append(field.name());
        builder.append(":");
        builder.append(schemaToStringDetailed(field.schema()));
      }
      builder.append("}");
    }

    return builder.toString();
  }

  private String valueToJson(Schema schema, Object value) {
    if (value == null) {
      return "null";
    }

    var builder = new StringBuilder();
    if (schema.type() == Schema.Type.STRUCT) {
      builder.append("{");
      for (var field : schema.fields()) {
        if (field.index() != 0) {
          builder.append(", ");
        }
        builder.append(field.name());
        builder.append(": ");
        builder.append(valueToJson(field.schema(), ((Struct) value).get(field)));
      }
      builder.append("}");
      return builder.toString();
    }
    if (schema.type() == Schema.Type.ARRAY) {
      builder.append("[");
      var array = (List<Object>) value;
      for (int i = 0; i < array.size(); ++i) {
        if (i != 0) {
          builder.append(", ");
        }
        builder.append(valueToJson(schema.valueSchema(), array.get(i)));
      }
      builder.append("]");
      return builder.toString();
    }

    if (schema.type() == Schema.Type.STRING) {
      return "\"" + value.toString() + "\"";
    }
    return value.toString();
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      System.out.println("");
      System.out.println(record.toString());

      if (record.valueSchema() == null) {
        System.out.println("null");
      } else {
        System.out.println("Schema{" + schemaToStringDetailed(record.valueSchema()) + "}");
      }

      // System.out.println(record.value().toString());
      System.out.println(valueToJson(record.valueSchema(), record.value()));
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {}

  @Override
  public void stop() {}
}
