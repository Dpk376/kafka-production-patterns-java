package com.example.kafka.inbox.domain;

public record OrderEvent(String orderId, String customerId, Double amount, String status) {}
