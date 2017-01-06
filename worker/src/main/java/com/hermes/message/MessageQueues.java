package com.hermes.message;

import com.hermes.connection.ConsumerConnectionsManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class MessageQueues {
    private ConcurrentHashMap<String, GroupMessageQueues> channelMessages;

    public MessageQueues() {
        this.channelMessages = new ConcurrentHashMap<>();
    }

    public synchronized void add(String channelName, Message message) {
        createQueueIfNotExists(channelName);
        channelMessages.get(channelName).add(message);
    }

    public synchronized LinkedBlockingQueue<Message> getQueueCreateIfNotExists(String channelName, String groupName) {
        createQueueIfNotExists(channelName);
        return channelMessages.get(channelName).getGroupCreateIfNotExists(groupName);
    }

    private void createQueueIfNotExists(String channelName) {
        if (!channelMessages.containsKey(channelName)) {
            channelMessages.put(channelName, new GroupMessageQueues());
        }
    }

    public synchronized void removeUnused(String channelName, String groupName,
                             ConsumerConnectionsManager consumerConnectionsManager) {
        if (channelName == null || groupName == null) {
            return;
        }
        if (consumerConnectionsManager.getConnections(channelName, groupName).size() == 0) {
            channelMessages.get(channelName).clear();
        }
        if (consumerConnectionsManager.getNumGroups(channelName) == 0) {
            channelMessages.remove(channelName);
        }
    }
}
