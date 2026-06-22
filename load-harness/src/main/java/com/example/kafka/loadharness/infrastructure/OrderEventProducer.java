package com.example.kafka.loadharness.infrastructure;

import com.example.kafka.loadharness.domain.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class OrderEventProducer {

  private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
  private static final String TOPIC = "orders.v1";

  private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

  public OrderEventProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void send(OrderEvent event) {
    log.debug("Producing order event: {}", event.orderId());
    var unused = kafkaTemplate.send(TOPIC, event.orderId().toString(), event);
  }

  public void sendPoisonPill() {
    log.debug("Producing poison pill message");
    // Send an event with status POISON_PILL that consumers should handle or fail on
    OrderEvent poison = new OrderEvent(UUID.randomUUID(), "poison-customer", 0.0, "POISON_PILL");
    var unused = kafkaTemplate.send(TOPIC, poison.orderId().toString(), poison);
  }
}
