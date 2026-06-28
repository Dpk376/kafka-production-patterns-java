package com.example.kafka.idempotent.application;

import com.example.kafka.common.idempotent.application.IdempotentMessageProcessor;
import com.example.kafka.idempotent.domain.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderApplicationService {

  private static final Logger log = LoggerFactory.getLogger(OrderApplicationService.class);

  private final IdempotentMessageProcessor idempotentProcessor;

  public OrderApplicationService(IdempotentMessageProcessor idempotentProcessor) {
    this.idempotentProcessor = idempotentProcessor;
  }

  /** Processes an order idempotently using the extracted generic processor. */
  public void processOrder(OrderEvent event, String dedupKey) {
    idempotentProcessor.process(
        dedupKey,
        () -> {
          // Business logic here, shielded from idempotency mechanics
          log.info("Processing order for first time: {}", event.orderId());
        });
  }
}
