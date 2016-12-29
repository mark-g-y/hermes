package com.hermes.network.timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class PacketTimeoutNode implements Delayed {
    public String messageId;
    public long timeoutTime;
    public CompletableFuture future;

    public PacketTimeoutNode(String messageId, long timeoutTime, CompletableFuture<Void> future) {
        this.messageId = messageId;
        this.timeoutTime = timeoutTime;
        this.future = future;
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
        return timeUnit.convert(timeoutTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed delayed) {
        PacketTimeoutNode other = (PacketTimeoutNode)delayed;
        if (this.timeoutTime < other.timeoutTime) {
            return -1;
        } else if (this.timeoutTime > other.timeoutTime) {
            return 1;
        } else {
            return 0;
        }
    }
}
