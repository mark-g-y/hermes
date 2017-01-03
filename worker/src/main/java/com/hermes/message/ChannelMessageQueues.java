package com.hermes.message;

import com.hermes.network.packet.MessagePacket;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ChannelMessageQueues {
    private ConcurrentHashMap<String, LinkedBlockingQueue<MessagePacket>> channelMessages;

    public ChannelMessageQueues() {
        this.channelMessages = new ConcurrentHashMap<>();
    }

    public synchronized void add(String channelName, MessagePacket packet) {
        if (!channelMessages.containsKey(channelName)) {
            channelMessages.put(channelName, new LinkedBlockingQueue<>());
        }
        channelMessages.get(channelName).add(packet);
    }

    public synchronized LinkedBlockingQueue<MessagePacket> getMessageQueue(String channelName) {
        return channelMessages.get(channelName);
    }
}
