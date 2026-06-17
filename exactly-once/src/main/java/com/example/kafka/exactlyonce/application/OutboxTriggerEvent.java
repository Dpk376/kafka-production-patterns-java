package com.example.kafka.exactlyonce.application;

import org.springframework.context.ApplicationEvent;

public class OutboxTriggerEvent extends ApplicationEvent {
  public OutboxTriggerEvent(Object source) {
    super(source);
  }
}
