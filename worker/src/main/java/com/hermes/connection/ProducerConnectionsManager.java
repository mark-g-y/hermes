package com.hermes.connection;

import com.hermes.network.SocketServerHandler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ProducerConnectionsManager {
    private ConcurrentHashMap<String, CopyOnWriteArrayList<SocketServerHandler>> connections;
    
    public ProducerConnectionsManager() {
        connections = new ConcurrentHashMap<>();
    }

    public List<SocketServerHandler> getConnections(String channelName) {
        return connections.get(channelName);
    }

    public List<String> getChannels() {
        List<String> channels = new ArrayList<>();
        Iterator<String> iterator = connections.keySet().iterator();
        while (iterator.hasNext()) {
            channels.add(iterator.next());
        }
        return channels;
    }

    public synchronized void add(String channelName, SocketServerHandler socketServerHandler) {
        if (!connections.containsKey(channelName)) {
            connections.put(channelName, new CopyOnWriteArrayList<>());
        }
        connections.get(channelName).add(socketServerHandler);
    }

    public synchronized boolean remove(String channelName, SocketServerHandler socketServerHandler) {
        boolean result = false;
        if (channelName != null && connections.containsKey(channelName)) {
            result = connections.get(channelName).remove(socketServerHandler);
            if (connections.get(channelName).isEmpty()) {
                connections.remove(channelName);
            }
        }
        return result;
    }
}
