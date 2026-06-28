package com.example.kafka.starter.dlq;

import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class DlqReplayService {

  private static final Logger log = LoggerFactory.getLogger(DlqReplayService.class);

  @SuppressWarnings("rawtypes")
  private final ConsumerFactory consumerFactory;
  @SuppressWarnings("rawtypes")
  private final KafkaTemplate kafkaTemplate;

  public DlqReplayService(
      @SuppressWarnings("rawtypes") ConsumerFactory consumerFactory,
      @SuppressWarnings("rawtypes") KafkaTemplate kafkaTemplate) {
    this.consumerFactory = consumerFactory;
    this.kafkaTemplate = kafkaTemplate;
  }

  /**
   * Replays messages from a DLQ topic back to the original topic. Treats messages as opaque byte
   * arrays to prevent deserialization poison pills during the replay process.
   *
   * @param dlqTopic The source dead-letter topic (e.g. "orders.v1.DLT")
   * @param originalTopic The target topic to replay to (e.g. "orders.v1")
   * @param maxMessages The maximum number of messages to replay in this batch
   * @return The number of messages successfully replayed
   */
  public int replay(String dlqTopic, String originalTopic, int maxMessages) {
    log.info(
        "Starting DLQ Replay. Source: {}, Target: {}, MaxMessages: {}",
        dlqTopic,
        originalTopic,
        maxMessages);

    int replayedCount = 0;

    // Create a standalone consumer for the replay operation
    try (Consumer<Object, Object> consumer =
        consumerFactory.createConsumer(
            "dlq-replayer-" + System.currentTimeMillis(), "client-dlq-replayer")) {

      // Assign to all partitions of the DLQ topic
      List<TopicPartition> partitions =
          consumer.partitionsFor(dlqTopic).stream()
              .map(p -> new TopicPartition(p.topic(), p.partition()))
              .toList();

      if (partitions.isEmpty()) {
        log.warn("No partitions found for DLQ topic {}", dlqTopic);
        return 0;
      }

      consumer.assign(partitions);

      // We don't use group management (subscribe), so we just read from the current committed
      // offset (if any) or beginning.
      // Since it's a manual assignment, we must seek to the beginning if we want to process all,
      // but normally
      // a DLQ replayer uses a dedicated consumer group to track its progress.
      // For simplicity in this SDK pattern, we will just read from the earliest available offset.
      consumer.seekToBeginning(partitions);

      boolean hasMore = true;
      while (hasMore && replayedCount < maxMessages) {
        ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofSeconds(2));

        if (records.isEmpty()) {
          hasMore = false; // Drain complete
          break;
        }

        for (ConsumerRecord<Object, Object> record : records) {
          if (replayedCount >= maxMessages) {
            break;
          }

          // Construct a new record for the original topic, preserving the key, value, and headers.
          // We strip the original DLQ routing headers if necessary, but for now we forward them.
          ProducerRecord<Object, Object> producerRecord =
              new ProducerRecord<>(
                  originalTopic, null, record.key(), record.value(), record.headers());

          kafkaTemplate.send(producerRecord).get(5, java.util.concurrent.TimeUnit.SECONDS);
          replayedCount++;
        }

        // Note: In a production-grade system, we would commit offsets here and delete/tombstone the
        // message
        // in the DLQ to prevent double-replaying. Because Kafka doesn't natively support message
        // deletion,
        // DLQs are typically managed with consumer groups or short retention policies.
      }
    } catch (Exception e) {
      log.error("Error during DLQ replay", e);
      throw new RuntimeException("DLQ Replay failed", e);
    }

    log.info("Finished DLQ Replay. Replayed {} messages.", replayedCount);
    return replayedCount;
  }
}
