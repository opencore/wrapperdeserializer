package com.opencore.kafka;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class InvalidFilterInterceptor implements ConsumerInterceptor<Object, Object> {

  @Override
  public ConsumerRecords<Object, Object> onConsume(
      ConsumerRecords<Object, Object> consumerRecords) {
    // Iterate over all contained partitions
    for (TopicPartition partition : consumerRecords.partitions()) {
      // Retrieve list of records for this partition
      List<ConsumerRecord<Object, Object>> recordsForPartition = consumerRecords.records(partition);
      for (int i = 0; i < recordsForPartition.size(); i++) {
        // Remove records with null value and key from the list
        if (recordsForPartition.get(i).key() == null && recordsForPartition.get(i).value() == null) {
          recordsForPartition.remove(i);
        }
      }
    }
    return consumerRecords;
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
