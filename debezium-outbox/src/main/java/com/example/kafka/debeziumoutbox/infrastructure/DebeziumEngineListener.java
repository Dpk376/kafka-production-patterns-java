package com.example.kafka.debeziumoutbox.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class DebeziumEngineListener {

  private static final Logger log = LoggerFactory.getLogger(DebeziumEngineListener.class);

  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final DebeziumEngine<ChangeEvent<String, String>> engine;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper objectMapper;
  private final OutboxEventRepository outboxEventRepository;

  public DebeziumEngineListener(
      io.debezium.config.Configuration debeziumConfiguration,
      KafkaTemplate<String, String> kafkaTemplate,
      ObjectMapper objectMapper,
      OutboxEventRepository outboxEventRepository) {
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
    this.outboxEventRepository = outboxEventRepository;

    this.engine =
        DebeziumEngine.create(Json.class)
            .using(debeziumConfiguration.asProperties())
            .notifying(this::handleChangeEvent)
            .build();
  }

  @PostConstruct
  private void start() {
    this.executor.execute(engine);
  }

  @PreDestroy
  private void stop() throws IOException {
    if (this.engine != null) {
      this.engine.close();
    }
    this.executor.shutdown();
  }

  private void handleChangeEvent(ChangeEvent<String, String> changeEvent) {
    if (changeEvent.value() == null) {
      return;
    }

    try {
      JsonNode payload = objectMapper.readTree(changeEvent.value());
      JsonNode payloadElement = payload.get("payload");

      if (payloadElement == null || payloadElement.isNull()) {
        return;
      }

      String operation = payloadElement.get("op").asText();

      // Only process INSERT operations ('c' for create)
      if (!"c".equals(operation)) {
        return;
      }

      JsonNode after = payloadElement.get("after");

      String topic = after.get("topic").asText();
      String key = after.get("message_key").asText();
      String eventPayload = after.get("payload").asText();

      // Publish to Kafka synchronously to guarantee exactly-once delivery
      try {
        kafkaTemplate.send(topic, key, eventPayload).get(5, java.util.concurrent.TimeUnit.SECONDS);
        log.info("Successfully published outbox event to Kafka topic: {}", topic);

        // Mark as published in DB
        java.util.UUID eventId = java.util.UUID.fromString(after.get("event_id").asText());
        outboxEventRepository.markPublished(eventId);
      } catch (Exception e) {
        log.error(
            "Failed to publish outbox event to Kafka. Halting Debezium to prevent data loss.", e);
        throw new RuntimeException("Kafka publish failed", e);
      }

    } catch (Exception e) {
      log.error("Error processing Debezium CDC event", e);
    }
  }
}
