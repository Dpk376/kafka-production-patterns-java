package com.example.kafka.inbox.infrastructure;

import com.example.kafka.inbox.application.OrderApplicationService;
import com.example.kafka.inbox.domain.OrderEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class InboxProcessorScheduler {

    private static final Logger log = LoggerFactory.getLogger(InboxProcessorScheduler.class);

    private final InboxMessageRepository inboxRepository;
    private final OrderApplicationService applicationService;
    private final ObjectMapper objectMapper;

    public InboxProcessorScheduler(InboxMessageRepository inboxRepository,
                                   OrderApplicationService applicationService,
                                   ObjectMapper objectMapper) {
        this.inboxRepository = inboxRepository;
        this.applicationService = applicationService;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void processInbox() {
        // Find unprocessed messages, lock them to prevent concurrent instances from grabbing them
        List<InboxMessage> messages = inboxRepository.findUnprocessed(PageRequest.of(0, 50));
        
        if (!messages.isEmpty()) {
            log.debug("Found {} unprocessed inbox messages", messages.size());
        }

        for (InboxMessage message : messages) {
            try {
                OrderEvent event = objectMapper.readValue(message.getPayload(), OrderEvent.class);
                applicationService.processOrder(event);
                
                message.markProcessed();
                inboxRepository.save(message); // Save the processed_at update
                log.info("Processed inbox message for key: {}", message.getMessageId());
            } catch (JsonProcessingException e) {
                log.error("Failed to deserialize message for key: {}", message.getMessageId(), e);
                // Depending on requirements, we might want to mark it as dead-lettered
                // For now, mark it as processed to skip it, or add a 'failed_at' column.
                message.markProcessed();
                inboxRepository.save(message);
            } catch (Exception e) {
                log.error("Failed to process business logic for key: {}. Will retry next polling cycle.", message.getMessageId(), e);
                // We do NOT mark it processed, so it's picked up again
            }
        }
    }
}
