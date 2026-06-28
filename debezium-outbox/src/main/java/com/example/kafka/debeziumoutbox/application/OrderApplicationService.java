package com.example.kafka.debeziumoutbox.application;

import com.example.kafka.starter.annotation.TransactionalOutbox;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class OrderApplicationService {

  /**
   * Simulates saving an Order to the database and writing a corresponding outbox event in the SAME
   * transaction. By using @TransactionalOutbox, the SDK automatically handles the JSON
   * serialization and database persistence, removing boilerplate from the business service.
   */
  @TransactionalOutbox(
      topic = "orders.v1",
      aggregateType = "Order",
      aggregateIdExpression = "#result.orderId().toString()",
      messageKeyExpression = "#result.orderId().toString()")
  public OrderCreatedEvent createOrder(UUID orderId, String customerId) {
    // 1. In a real app, we would save the domain entity here
    // orderRepository.save(new Order(orderId, customerId));

    // 2. Return the domain event. The starter's Aspect will intercept this,
    // serialize it, and persist it to the outbox table within the same transaction.
    return new OrderCreatedEvent(orderId, customerId, "CREATED");
  }

  public record OrderCreatedEvent(UUID orderId, String customerId, String status) {}
}
