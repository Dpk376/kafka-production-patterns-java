package com.example.kafka.exactlyonce.application;

import com.example.kafka.exactlyonce.infrastructure.OutboxEvent;
import com.example.kafka.exactlyonce.infrastructure.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderApplicationService {

  private final OutboxEventRepository outboxEventRepository;
  private final ObjectMapper objectMapper;
  private final ApplicationEventPublisher eventPublisher;

  public OrderApplicationService(
      OutboxEventRepository outboxEventRepository,
      ObjectMapper objectMapper,
      ApplicationEventPublisher eventPublisher) {
    this.outboxEventRepository = outboxEventRepository;
    this.objectMapper = objectMapper;
    this.eventPublisher = eventPublisher;
  }

  /**
   * Simulates saving an Order to the database and writing a corresponding outbox event in the SAME
   * transaction.
   */
  @Transactional
  public void createOrder(UUID orderId, String customerId) {
    // 1. In a real app, we would save the domain entity here
    // orderRepository.save(new Order(orderId, customerId));

    // 2. Write the outbox event in the same transaction
    try {
      String payload =
          objectMapper.writeValueAsString(new OrderCreatedEvent(orderId, customerId, "CREATED"));

      OutboxEvent event =
          new OutboxEvent(
              UUID.randomUUID(),
              "Order",
              orderId.toString(),
              "orders.v1",
              orderId.toString(),
              payload);

      outboxEventRepository.save(event);
      eventPublisher.publishEvent(new OutboxTriggerEvent(this));

    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize event", e);
    }
  }

  public record OrderCreatedEvent(UUID orderId, String customerId, String status) {}
}
