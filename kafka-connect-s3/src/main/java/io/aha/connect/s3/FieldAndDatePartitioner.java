package io.aha.connect.s3;

import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Create partitions by combining the results from FieldPartitioner and
 * TimeBasedPartitioner.
 */
public class FieldAndDatePartitioner<T> extends DefaultPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(FieldAndDatePartitioner.class);
  private FieldPartitioner<T> fieldPartitioner;
  private TimeBasedPartitioner<T> timeBasedPartitioner;

  public FieldAndDatePartitioner() {
  }

  @Override
  public void configure(Map<String, Object> config) {
    super.configure(config);
    fieldPartitioner = new FieldPartitioner<>();
    fieldPartitioner.configure(config);
    timeBasedPartitioner = new TimeBasedPartitioner<>();
    timeBasedPartitioner.configure(config);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    String fieldPartition = fieldPartitioner.encodePartition(sinkRecord);
    String timePartition = timeBasedPartitioner.encodePartition(sinkRecord);
    log.info("fieldPartition: {}, timePartition: {}", fieldPartition, timePartition);
    return fieldPartition + delim + timePartition;
  }

  @Override
  public List<T> partitionFields() {
    return Stream.concat(
      fieldPartitioner.partitionFields().stream(),
        timeBasedPartitioner.partitionFields().stream()
      )
      .collect(Collectors.toList());
  }
}
