package com.example.kafka.loadharness.application;

import com.example.kafka.loadharness.domain.OrderEvent;
import com.example.kafka.loadharness.infrastructure.OrderEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/load")
public class TrafficGeneratorController {

  private static final Logger log = LoggerFactory.getLogger(TrafficGeneratorController.class);
  private final OrderEventProducer producer;

  public TrafficGeneratorController(OrderEventProducer producer) {
    this.producer = producer;
  }

  @PostMapping("/generate")
  public String generateTraffic(@RequestParam(defaultValue = "10") int count) {
    log.info("Generating {} order events", count);
    for (int i = 0; i < count; i++) {
      OrderEvent event = new OrderEvent(
          UUID.randomUUID(),
          "customer-" + (int)(Math.random() * 1000),
          Math.round(Math.random() * 1000.0 * 100.0) / 100.0,
          "CREATED"
      );
      producer.send(event);
    }
    return "Generated " + count + " events";
  }

  @PostMapping("/poison")
  public String generatePoisonPill() {
    log.info("Generating a poison pill event");
    producer.sendPoisonPill();
    return "Poison pill sent";
  }
}
