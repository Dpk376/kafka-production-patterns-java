package com.example.kafka.exactlyonce;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ExactlyOnceApplication {

  public static void main(String[] args) {
    SpringApplication.run(ExactlyOnceApplication.class, args);
  }
}
