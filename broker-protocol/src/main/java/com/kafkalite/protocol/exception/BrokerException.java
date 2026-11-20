package com.kafkalite.protocol.exception;

import com.kafkalite.protocol.error.ErrorCode;

public class BrokerException extends RuntimeException {
    private final ErrorCode errorCode;

    public BrokerException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}
