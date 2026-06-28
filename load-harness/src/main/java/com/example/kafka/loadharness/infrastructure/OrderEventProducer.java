package com.example.kafka.loadharness.infrastructure;

import com.example.kafka.common.avro.OrderEvent;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderEventProducer {

  private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
  private static final String TOPIC = "orders.v1";

  private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

  public OrderEventProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void send(OrderEvent event) {
    log.debug("Producing order event: {}", event.getOrderId());
    var unused = kafkaTemplate.send(TOPIC, event.getOrderId().toString(), event);
  }

  public void sendPoisonPill() {
    log.debug("Producing poison pill message");
    // Send an event with status POISON_PILL that consumers should handle or fail on
    OrderEvent poison =
        OrderEvent.newBuilder()
            .setOrderId(UUID.randomUUID().toString())
            .setCustomerId("poison-customer")
            .setPrice(0.0)
            .setStatus("POISON_PILL")
            .build();
    var unused = kafkaTemplate.send(TOPIC, poison.getOrderId().toString(), poison);
  }
}
