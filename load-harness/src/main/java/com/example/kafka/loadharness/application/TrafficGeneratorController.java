package com.example.kafka.loadharness.application;

import com.example.kafka.common.avro.OrderEvent;
import com.example.kafka.loadharness.infrastructure.OrderEventProducer;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
      OrderEvent event =
          OrderEvent.newBuilder()
              .setOrderId(UUID.randomUUID().toString())
              .setCustomerId("customer-" + (int) (Math.random() * 1000))
              .setPrice(Math.round(Math.random() * 1000.0 * 100.0) / 100.0)
              .setStatus("CREATED")
              .build();
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
