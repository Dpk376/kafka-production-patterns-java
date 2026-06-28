package com.example.kafka.idempotent.application;

import com.example.kafka.common.avro.OrderEvent;
import com.example.kafka.common.idempotent.application.IdempotentMessageProcessor;
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
          // Here, actual business logic would be executed.
          log.info(
              "Executing business logic for order {}. Amount: {}, Status: {}",
              event.getOrderId(),
              event.getPrice(),
              event.getStatus());
        });
  }
}
