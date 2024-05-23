package io.github.ligfx;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public class LoggingSinkTask extends SinkTask {
  @Override
  public String version() {
    return new LoggingSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {}

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      System.out.println("");
      System.out.println(record.toString());
      System.out.println(record.value().toString());
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {}

  @Override
  public void stop() {}
}
