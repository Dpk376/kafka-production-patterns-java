package com.example.kafka.dlq.infrastructure;

import com.example.kafka.dlq.application.OrderApplicationService;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

  private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
  private final OrderApplicationService applicationService;
  private final DeadLetterRepository deadLetterRepository;

  public OrderEventConsumer(
      OrderApplicationService applicationService, DeadLetterRepository deadLetterRepository) {
    this.applicationService = applicationService;
    this.deadLetterRepository = deadLetterRepository;
  }

  @KafkaListener(topics = "orders.v1", groupId = "dlq-consumer-group")
  public void listen(ConsumerRecord<String, String> record) {
    log.debug("Received message: {}", record.value());
    applicationService.processOrder(record.value());
  }

  /**
   * Optional: Consumer specifically designed to read from the DLT, classify, alert, or replay the
   * messages manually.
   */
  @KafkaListener(topics = "orders.v1.DLT", groupId = "dlq-inspector-group")
  public void listenDlt(ConsumerRecord<String, String> record) {
    String payload = record.value();

    String exceptionMessage = "No exception message provided";
    var exceptionHeader = record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);
    if (exceptionHeader != null) {
      exceptionMessage = new String(exceptionHeader.value(), StandardCharsets.UTF_8);
    }

    log.warn(
        "DLQ INSPECTOR: Received dead letter record: {}. Exception: {}", payload, exceptionMessage);

    deadLetterRepository.save(new DeadLetter(record.topic(), payload, exceptionMessage));
  }
}
