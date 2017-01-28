package com.hermes;

/**
 * Interface for handling message deliveries.
 */
public interface Receiver {
    /**
     * Invoked when a message is received.
     * @param message   The message as a string.
     */
    void onMessageReceived(String message);
}
