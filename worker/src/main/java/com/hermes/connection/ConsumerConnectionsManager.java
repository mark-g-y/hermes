package com.hermes.connection;

import com.hermes.network.SocketServerHandlerThread;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class ConsumerConnectionsManager {
    private ConcurrentHashMap<String, ConcurrentHashMap<String, List<SocketServerHandlerThread>>> connections;

    public ConsumerConnectionsManager() {
        connections = new ConcurrentHashMap<>();
    }

    public synchronized List<SocketServerHandlerThread> getConnections(String channelName, String groupName) {
        createConnectionsListIfNotExist(channelName, groupName);
        return connections.get(channelName).get(groupName);
    }

    public synchronized int getNumGroups(String channelName) {
        if (connections.get(channelName) == null) {
            return 0;
        }
        return connections.get(channelName).keySet().size();
    }

    public synchronized List<String> getChannels() {
        return connections.keySet().stream().collect(Collectors.toList());
    }

    public synchronized void add(String channelName, String groupName, SocketServerHandlerThread socketServerHandlerThread) {
        createConnectionsListIfNotExist(channelName, groupName);
        connections.get(channelName).get(groupName).add(socketServerHandlerThread);
    }

    public synchronized boolean remove(String channelName, String groupName, SocketServerHandlerThread socketServerHandlerThread) {
        boolean result = false;
        if (channelName != null && groupName != null && connections.containsKey(channelName)) {
            result = connections.get(channelName).get(groupName).remove(socketServerHandlerThread);
            if (connections.get(channelName).get(groupName).isEmpty()) {
                connections.get(channelName).remove(groupName);
            }
            if (connections.get(channelName).isEmpty()) {
                connections.remove(channelName);
            }
        }
        return result;
    }

    private void createConnectionsListIfNotExist(String channelName, String groupName) {
        if (!connections.containsKey(channelName)) {
            connections.put(channelName, new ConcurrentHashMap<>());
        }
        if (!connections.get(channelName).containsKey(groupName)) {
            connections.get(channelName).put(groupName, new CopyOnWriteArrayList<>());
        }
    }
}
