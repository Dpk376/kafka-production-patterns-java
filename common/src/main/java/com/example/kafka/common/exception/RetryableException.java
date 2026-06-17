package com.example.kafka.common.exception;

public final class RetryableException extends ProcessingException {

  public RetryableException(String message) {
    super(message);
  }

  public RetryableException(String message, Throwable cause) {
    super(message, cause);
  }
}
