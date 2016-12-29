package com.hermes;

public interface ProducerCallback {
    void onSuccess();
    void onFailure(Throwable th);
}
