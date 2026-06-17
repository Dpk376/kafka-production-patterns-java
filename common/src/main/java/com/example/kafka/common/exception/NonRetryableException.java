package com.example.kafka.common.exception;

public final class NonRetryableException extends ProcessingException {

  public NonRetryableException(String message) {
    super(message);
  }

  public NonRetryableException(String message, Throwable cause) {
    super(message, cause);
  }
}
