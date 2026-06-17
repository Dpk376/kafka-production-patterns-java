package com.example.kafka.idempotent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IdempotentConsumerApplication {

  public static void main(String[] args) {
    SpringApplication.run(IdempotentConsumerApplication.class, args);
  }
}
