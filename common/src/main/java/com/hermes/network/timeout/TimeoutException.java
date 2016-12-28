package com.hermes.network.timeout;

public class TimeoutException extends RuntimeException {
    private String message;

    public TimeoutException(String message) {
        super(message);
    }
}
