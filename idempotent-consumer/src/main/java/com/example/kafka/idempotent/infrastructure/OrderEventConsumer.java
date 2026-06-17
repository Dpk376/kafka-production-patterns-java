package com.example.kafka.idempotent.infrastructure;

import com.example.kafka.common.exception.NonRetryableException;
import com.example.kafka.idempotent.application.OrderApplicationService;
import com.example.kafka.idempotent.domain.OrderEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
  private final ObjectMapper objectMapper;

  public OrderEventConsumer(OrderApplicationService applicationService, ObjectMapper objectMapper) {
    this.applicationService = applicationService;
    this.objectMapper = objectMapper;
  }

  @KafkaListener(
      topics = "orders.v1",
      groupId = "idempotent-consumer-group",
      containerFactory = "kafkaListenerContainerFactory")
  public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
    log.info(
        "Received message key: {}, partition: {}, offset: {}",
        record.key(),
        record.partition(),
        record.offset());

    try {
      OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class);

      // The dedupKey combines topic, partition, and offset to uniquely identify a message instance.
      // A business key (e.g. orderId) could also be used depending on deduplication scope.
      String dedupKey = record.topic() + "-" + record.partition() + "-" + record.offset();

      applicationService.processOrder(event, dedupKey);

      // Only acknowledge Kafka after successful DB commit
      ack.acknowledge();

    } catch (JsonProcessingException e) {
      log.error("Failed to deserialize message: {}", record.value(), e);
      throw new NonRetryableException("Invalid JSON format", e);
    }
  }
}
