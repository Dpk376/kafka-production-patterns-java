package com.example.kafka.common.idempotent.application;

import com.example.kafka.common.idempotent.infrastructure.ProcessedMessage;
import com.example.kafka.common.idempotent.infrastructure.ProcessedMessageRepository;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IdempotentMessageProcessor {

  private static final Logger log = LoggerFactory.getLogger(IdempotentMessageProcessor.class);
  private final ProcessedMessageRepository repository;

  public IdempotentMessageProcessor(ProcessedMessageRepository repository) {
    this.repository = repository;
  }

  /** Executes the given action idempotently based on the dedupKey. */
  @Transactional
  public void process(String dedupKey, Runnable action) {
    try {
      repository.saveAndFlush(new ProcessedMessage(dedupKey, OffsetDateTime.now(ZoneOffset.UTC)));
      action.run();
    } catch (DataIntegrityViolationException e) {
      log.warn("Duplicate message detected, skipping processing. Dedup Key: {}", dedupKey);
    }
  }
}
