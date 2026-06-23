package com.example.kafka.inbox.infrastructure;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    private final InboxMessageRepository inboxRepository;

    public OrderEventConsumer(InboxMessageRepository inboxRepository) {
        this.inboxRepository = inboxRepository;
    }

    @KafkaListener(topics = "orders.events", groupId = "inbox-group")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String dedupKey = record.key(); // using the business key as the dedup key
        String payload = record.value();
        
        log.info("Received order event for key: {}", dedupKey);

        try {
            inboxRepository.save(new InboxMessage(dedupKey, payload));
            log.info("Successfully persisted event to inbox for key: {}", dedupKey);
        } catch (DataIntegrityViolationException e) {
            log.warn("Duplicate message detected for key: {}. Discarding.", dedupKey);
            // We caught the unique constraint violation, meaning it's a duplicate.
            // We just swallow it so we can ack the message.
        }

        // Always acknowledge manual offset commit
        ack.acknowledge();
    }
}
