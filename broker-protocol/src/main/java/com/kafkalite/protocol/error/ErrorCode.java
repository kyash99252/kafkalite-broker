package com.kafkalite.protocol.error;

public enum ErrorCode {
  NONE((short) 0),
  UNKNOWN_ERROR((short) 1),
  INVALID_REQUEST((short) 2),
  UNKNOWN_TOPIC((short) 3),
  TOPIC_ALREADY_EXISTS((short) 4),
  INVALID_PARTITION((short) 5),
  OFFSET_OUT_OF_RANGE((short) 6),
  CORRUPT_RECORD((short) 7),
  UNKNOWN_CONSUMER_GROUP((short) 8),
  REBALANCE_IN_PROGRESS((short) 9),
  STORAGE_FAILURE((short) 10);

  private final short code;

  ErrorCode(short code) {
    this.code = code;
  }

  public short code() {
    return code;
  }
}
