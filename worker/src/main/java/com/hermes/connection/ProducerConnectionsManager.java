package com.hermes.connection;

import com.hermes.network.SocketServerHandlerThread;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ProducerConnectionsManager {
    private ConcurrentHashMap<String, CopyOnWriteArrayList<SocketServerHandlerThread>> connections;
    
    public ProducerConnectionsManager() {
        connections = new ConcurrentHashMap<>();
    }

    public List<SocketServerHandlerThread> getConnections(String channelName) {
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
