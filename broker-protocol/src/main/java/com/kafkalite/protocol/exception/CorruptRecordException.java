package com.kafkalite.protocol.exception;

public class CorruptRecordException extends RuntimeException {
  public CorruptRecordException(String message) {
    super(message);
  }
}
