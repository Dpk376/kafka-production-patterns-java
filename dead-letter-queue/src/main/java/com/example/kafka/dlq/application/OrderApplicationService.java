package com.example.kafka.dlq.application;

import com.example.kafka.common.exception.NonRetryableException;
import com.example.kafka.common.exception.RetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderApplicationService {

  private static final Logger log = LoggerFactory.getLogger(OrderApplicationService.class);

  public void processOrder(String payload) {
    log.info("Attempting to process payload: {}", payload);

    if (payload.contains("RETRY")) {
      log.warn("Encountered transient failure, throwing RetryableException");
      throw new RetryableException("Simulated transient network or DB timeout");
    }

    if (payload.contains("POISON")) {
      log.error("Encountered unparseable payload or invalid state, throwing NonRetryableException");
      throw new NonRetryableException("Simulated invalid state or malformed JSON");
    }

    log.info("Successfully processed payload: {}", payload);
  }
}
