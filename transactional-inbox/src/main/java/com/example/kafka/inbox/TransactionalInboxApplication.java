package com.example.kafka.inbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TransactionalInboxApplication {
  public static void main(String[] args) {
    SpringApplication.run(TransactionalInboxApplication.class, args);
  }
}
