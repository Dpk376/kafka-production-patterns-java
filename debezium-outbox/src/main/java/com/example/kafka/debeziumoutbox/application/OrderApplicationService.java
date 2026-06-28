package com.example.kafka.debeziumoutbox.application;

import com.example.kafka.debeziumoutbox.infrastructure.OutboxEvent;
import com.example.kafka.debeziumoutbox.infrastructure.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderApplicationService {

  private final OutboxEventRepository outboxEventRepository;
  private final ObjectMapper objectMapper;

  public OrderApplicationService(
      OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
    this.outboxEventRepository = outboxEventRepository;
    this.objectMapper = objectMapper;
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

    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize event", e);
    }
  }

  public record OrderCreatedEvent(UUID orderId, String customerId, String status) {}
}
