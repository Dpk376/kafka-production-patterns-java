package com.example.kafka.exactlyonce.infrastructure;

import com.example.kafka.exactlyonce.application.OutboxTriggerEvent;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class OutboxRelayScheduler {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

  private final OutboxEventRepository repository;
  private final KafkaTemplate<String, String> kafkaTemplate;

  public OutboxRelayScheduler(
      OutboxEventRepository repository, KafkaTemplate<String, String> kafkaTemplate) {
    this.repository = repository;
    this.kafkaTemplate = kafkaTemplate;
  }

  /**
   * Polls the outbox table for unpublished events and relays them to Kafka. Uses @Transactional to
   * ensure the lock (SKIP LOCKED) is held for the duration of the fetch-and-publish cycle.
   */
  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = true)
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handleOutboxTrigger(OutboxTriggerEvent event) {
    log.debug("OutboxTriggerEvent received, executing relay immediately.");
    relayEvents();
  }

  @Scheduled(fixedDelayString = "${outbox.relay.delay-ms:5000}")
  @Transactional
  public void relayEvents() {
    // Fetch up to 100 unpublished events, locking them to prevent other relay
    // instances from picking them up simultaneously.
    List<OutboxEvent> events = repository.findUnpublishedAndLock(PageRequest.of(0, 100));

    if (events.isEmpty()) {
      return;
    }

    log.info("Relaying {} events to Kafka", events.size());

    for (OutboxEvent event : events) {
      try {
        // Send to Kafka synchronously.
        // We must ensure the record is acknowledged by Kafka before we mark it published.
        SendResult<String, String> result =
            kafkaTemplate
                .send(event.getTopic(), event.getMessageKey(), event.getPayload())
                .get(5, TimeUnit.SECONDS);

        log.debug(
            "Published event {} to topic {} at offset {}",
            event.getEventId(),
            event.getTopic(),
            result.getRecordMetadata().offset());

        // Mark published. Update occurs at transaction commit.
        event.markPublished();

      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        // If Kafka fails, we abort the transaction.
        // The lock is released, and the record remains unpublished (will be retried on next poll).
        log.error("Failed to publish outbox event: {}", event.getEventId(), e);
        throw new RuntimeException("Relay failure", e);
      }
    }
  }
}
