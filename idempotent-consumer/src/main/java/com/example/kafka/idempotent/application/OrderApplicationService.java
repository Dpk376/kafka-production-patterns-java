package com.example.kafka.idempotent.application;

import com.example.kafka.idempotent.domain.OrderEvent;
import com.example.kafka.idempotent.infrastructure.ProcessedMessage;
import com.example.kafka.idempotent.infrastructure.ProcessedMessageRepository;
import java.time.OffsetDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderApplicationService {

  private static final Logger log = LoggerFactory.getLogger(OrderApplicationService.class);

  private final ProcessedMessageRepository processedMessageRepository;

  public OrderApplicationService(ProcessedMessageRepository processedMessageRepository) {
    this.processedMessageRepository = processedMessageRepository;
  }

  /**
   * Processes an order idempotently. Uses the database PRIMARY KEY constraint on processed_message
   * to guarantee exactly-once processing side-effects.
   */
  @Transactional
  public void processOrder(OrderEvent event, String dedupKey) {
    try {
      // Attempt to record the message as processed.
      // If another transaction already inserted this dedupKey,
      // the database constraint will throw a DataIntegrityViolationException.
      processedMessageRepository.saveAndFlush(
          new ProcessedMessage(dedupKey, OffsetDateTime.now(java.time.ZoneOffset.UTC)));

      // Proceed with business logic here since we are the first to process it.
      log.info("Processing order for first time: {}", event.orderId());

    } catch (DataIntegrityViolationException e) {
      // We caught a unique constraint violation, meaning this message was already processed.
      // We log and return normally (idempotent success).
      log.warn("Duplicate message detected, skipping processing. Dedup Key: {}", dedupKey);
    }
  }
}
