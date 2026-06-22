package com.example.kafka.loadharness.domain;

import java.util.UUID;

public record OrderEvent(UUID orderId, String customerId, Double amount, String status) {}
