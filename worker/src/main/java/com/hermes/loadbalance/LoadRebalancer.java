package com.hermes.loadbalance;

import com.hermes.connection.ChannelClientConnectionsManager;
import com.hermes.network.SocketServerHandlerThread;
import com.hermes.partition.Partition;
import com.hermes.worker.WorkerManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class LoadRebalancer {
    private ChannelClientConnectionsManager producerConnectionsManager;

    public LoadRebalancer(ChannelClientConnectionsManager producerConnectionsManager) {
        this.producerConnectionsManager = producerConnectionsManager;
    }

    public void rebalanceWorkers() {
        String partition = getHeaviestLoadPartition();
        if (partition == null) {
            return;
        }
        if (!allocateNewWorkerForPartition(partition)) {
            return;
        }

        // remove 1/2 connections from current worker - client load balancing will connect to less heavily loaded workers
        disconnectConnectionsForPartition(partition, 0.5);
    }

    private String getHeaviestLoadPartition() {
        // find most load-heavy partitions by counting most producer connections
        HashMap<String, Integer> numPartitionConnections = new HashMap<>();
        List<String> channels = producerConnectionsManager.getChannels();
        for (String channel : channels) {
            String partition = Partition.get(channel);
            if (!numPartitionConnections.containsKey(partition)) {
                numPartitionConnections.put(partition, 0);
            }
            numPartitionConnections.put(partition, numPartitionConnections.get(partition) +
                                                   producerConnectionsManager.getConnections(channel).size());
        }

        String heaviestLoadPartition = null;
        int heaviestLoad = 0;
        Iterator<String> partitions = numPartitionConnections.keySet().iterator();
        while (partitions.hasNext()) {
            String partition = partitions.next();
            if (numPartitionConnections.get(partition) > heaviestLoad) {
                heaviestLoad = numPartitionConnections.get(partition);
                heaviestLoadPartition = partition;
            }
        }
        return heaviestLoadPartition;
    }

    private boolean allocateNewWorkerForPartition(String partition) {
        try {
            WorkerManager.allocateWorkersForPartition(partition, 1, WorkerManager.getAllWorkersForPartition(partition));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private void disconnectConnectionsForPartition(String partition, double percentToRemove) {
        List<String> channels = producerConnectionsManager.getChannels();
        List<SocketServerHandlerThread> connections = new ArrayList<>();
        for (String channel : channels) {
            if (partition.equals(Partition.get(channel))) {
                connections.addAll(producerConnectionsManager.getConnections(channel));
            }
        }
        for (int i = 0; i < percentToRemove * connections.size(); i++) {
            connections.get(i).shutdown();
        }
    }
}
