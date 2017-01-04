package com.hermes.message;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ChannelMessageQueues {
    private ConcurrentHashMap<String, LinkedBlockingQueue<Message>> channelMessages;

    public ChannelMessageQueues() {
        this.channelMessages = new ConcurrentHashMap<>();
    }

    public synchronized void add(String channelName, Message message) {
        createQueueIfNotExists(channelName);
        channelMessages.get(channelName).add(message);
    }

    public synchronized LinkedBlockingQueue<Message> getQueueCreateIfNotExists(String channelName) {
        createQueueIfNotExists(channelName);
        return channelMessages.get(channelName);
    }

    private void createQueueIfNotExists(String channelName) {
        if (!channelMessages.containsKey(channelName)) {
            channelMessages.put(channelName, new LinkedBlockingQueue<>());
        }
    }
}
