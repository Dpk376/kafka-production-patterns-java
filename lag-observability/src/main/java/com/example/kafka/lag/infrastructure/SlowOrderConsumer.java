package com.example.kafka.lag.infrastructure;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SlowOrderConsumer {

  private static final Logger log = LoggerFactory.getLogger(SlowOrderConsumer.class);

  @KafkaListener(topics = "orders.v1", groupId = "lag-consumer-group")
  public void listen(ConsumerRecord<String, String> record) {
    try {
      // Artificially slow processing to simulate load and generate consumer lag
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    log.debug("Processed record: {}", record.key());
  }
}
