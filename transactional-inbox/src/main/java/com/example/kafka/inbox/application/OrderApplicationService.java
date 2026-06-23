package com.example.kafka.inbox.application;

import com.example.kafka.inbox.domain.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderApplicationService {

    private static final Logger log = LoggerFactory.getLogger(OrderApplicationService.class);

    // Business logic runs in its own transaction or joins the poller's transaction
    @Transactional(propagation = Propagation.REQUIRED)
    public void processOrder(OrderEvent event) {
        log.info("Executing business logic for order: {}", event.orderId());
        
        // Simulating some business logic like saving to the primary business tables
        // orderRepository.save(...)
    }
}
