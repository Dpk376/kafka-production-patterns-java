package com.example.kafka.common.exception;

public abstract sealed class ProcessingException extends RuntimeException
    permits RetryableException, NonRetryableException {

  public ProcessingException(String message) {
    super(message);
  }

  public ProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}
