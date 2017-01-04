package com.hermes.connection;

import com.hermes.network.SocketServerHandlerThread;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ChannelClientConnectionsManager {
    private ConcurrentHashMap<String, CopyOnWriteArrayList<SocketServerHandlerThread>> connections;
    
    public ChannelClientConnectionsManager() {
        connections = new ConcurrentHashMap<>();
    }

    public List<SocketServerHandlerThread> getConnections(String channelName) {
        return connections.get(channelName);
    }

    public synchronized void add(String channelName, SocketServerHandlerThread socketServerHandlerThread) {
        if (!connections.containsKey(channelName)) {
            connections.put(channelName, new CopyOnWriteArrayList<>());
        }
        connections.get(channelName).add(socketServerHandlerThread);
    }

    public synchronized boolean remove(String channelName, SocketServerHandlerThread socketServerHandlerThread) {
        boolean result = false;
        if (channelName != null && connections.containsKey(channelName)) {
            result = connections.get(channelName).remove(socketServerHandlerThread);
            if (connections.get(channelName).isEmpty()) {
                connections.remove(channelName);
            }
        }
        return result;
    }
}
