package com.example.kafka.idempotent.infrastructure;

import com.example.kafka.common.avro.OrderEvent;
import com.example.kafka.idempotent.application.OrderApplicationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

  private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

  private final OrderApplicationService applicationService;

  public OrderEventConsumer(OrderApplicationService applicationService) {
    this.applicationService = applicationService;
  }

  @KafkaListener(
      topics = "orders.v1",
      groupId = "idempotent-consumer-group",
      containerFactory = "kafkaListenerContainerFactory")
  public void listen(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
    log.info(
        "Received message key: {}, partition: {}, offset: {}",
        record.key(),
        record.partition(),
        record.offset());

    OrderEvent event = record.value();

    // The dedupKey combines topic, partition, and offset to uniquely identify a message instance.
    // A business key (e.g. orderId) could also be used depending on deduplication scope.
    String dedupKey = record.topic() + "-" + record.partition() + "-" + record.offset();

    applicationService.processOrder(event, dedupKey);

    // Only acknowledge Kafka after successful DB commit
    ack.acknowledge();
  }
}
