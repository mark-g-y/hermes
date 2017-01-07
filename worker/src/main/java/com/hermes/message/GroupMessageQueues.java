package com.hermes.message;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class GroupMessageQueues {
    private ConcurrentHashMap<String, LinkedBlockingQueue<Message>> groupMessages;

    public GroupMessageQueues() {
        this.groupMessages = new ConcurrentHashMap<>();
    }

    public synchronized LinkedBlockingQueue<Message> getGroupCreateIfNotExists(String groupName) {
        if (!groupMessages.containsKey(groupName)) {
            groupMessages.put(groupName, new LinkedBlockingQueue<>());
        }
        return groupMessages.get(groupName);
    }

    public synchronized void add(Message message) {
        for (String groupName : groupMessages.keySet()) {
            groupMessages.get(groupName).add(message);
        }
    }

    public synchronized void clear() {
        groupMessages.clear();
    }
}
