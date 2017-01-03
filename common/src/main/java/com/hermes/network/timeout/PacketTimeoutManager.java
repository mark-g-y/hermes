package com.hermes.network.timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

public class PacketTimeoutManager {
    private DelayQueue<PacketTimeoutNode> sentMessages;
    private ConcurrentHashMap<String, PacketTimeoutNode> messages;
    private Thread timeoutManagerThread;

    public PacketTimeoutManager() {
        sentMessages = new DelayQueue<>();
        messages = new ConcurrentHashMap<>();
        startTimeoutManagement();
    }

    public void add(String messageId, long timeout, CompletableFuture<Void> future) {
        PacketTimeoutNode node = new PacketTimeoutNode(messageId, System.currentTimeMillis() + timeout, future);
        messages.put(messageId, node);
        sentMessages.add(node);
    }

    public void messageReceived(String messageId) {
        try {
            messages.get(messageId).future.complete(null);
        } catch (NullPointerException e) {
            // message already deleted after timeout
        }
    }

    private void startTimeoutManagement() {
        timeoutManagerThread = new Thread(() -> {
            while(true) {
                try {
                    PacketTimeoutNode node = sentMessages.take();
                    node.future.completeExceptionally(new TimeoutException("Did not receive ack in time"));
                    messages.remove(node.messageId);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        timeoutManagerThread.start();
    }

    public void stop() {
        timeoutManagerThread.interrupt();
        for (PacketTimeoutNode timeoutNode : sentMessages) {
            timeoutNode.future.completeExceptionally(new java.util.concurrent.TimeoutException("Did not receive ack in time"));
        }
    }
}
