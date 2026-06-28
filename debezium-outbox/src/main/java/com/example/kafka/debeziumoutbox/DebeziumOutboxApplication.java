package com.example.kafka.debeziumoutbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DebeziumOutboxApplication {

  public static void main(String[] args) {
    SpringApplication.run(DebeziumOutboxApplication.class, args);
  }
}
