package com.example.kafka.starter.outbox;

import com.example.kafka.common.outbox.OutboxEvent;
import com.example.kafka.common.outbox.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class OutboxEventPublisher {

  private final OutboxEventRepository outboxEventRepository;
  private final ObjectMapper objectMapper;

  public OutboxEventPublisher(
      OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
    this.outboxEventRepository = outboxEventRepository;
    this.objectMapper = objectMapper;
  }

  public void publish(
      String topic, String aggregateType, String aggregateId, String messageKey, Object payload) {
    try {
      String jsonPayload = objectMapper.writeValueAsString(payload);

      OutboxEvent outboxEvent =
          new OutboxEvent(
              UUID.randomUUID(), aggregateType, aggregateId, topic, messageKey, jsonPayload);

      outboxEventRepository.save(outboxEvent);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize Outbox payload", e);
    }
  }
}
