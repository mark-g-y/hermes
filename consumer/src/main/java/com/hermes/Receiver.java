package com.hermes;

public interface Receiver {
    void onMessageReceived(String message);
    void onDisconnect(Throwable throwable);
}
