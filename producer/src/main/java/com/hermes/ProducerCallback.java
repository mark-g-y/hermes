package com.hermes;

/**
 * Callback for sending messages.
 */
public interface ProducerCallback {
    /**
     * Invoked when message is successfully delivered to the server.
     */
    void onSuccess();

    /**
     * Invoked when message is unsuccessfully delivered.
     * @param th    The exception that caused the unsuccessful delivery.
     */
    void onFailure(Throwable th);
}
