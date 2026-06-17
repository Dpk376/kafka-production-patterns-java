package com.example.kafka.dlq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DlqApplication {

  public static void main(String[] args) {
    SpringApplication.run(DlqApplication.class, args);
  }
}
