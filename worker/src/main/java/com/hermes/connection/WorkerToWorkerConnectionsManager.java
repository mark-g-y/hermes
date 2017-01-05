package com.hermes.connection;

import com.hermes.WorkerClient;
import com.hermes.worker.metadata.Worker;

import java.util.HashMap;

public class WorkerToWorkerConnectionsManager {
    private HashMap<String, WorkerClient> workerConnections;

    public WorkerToWorkerConnectionsManager() {
        this.workerConnections = new HashMap<>();
    }

    public synchronized WorkerClient getConnectionCreateIfNotExists(Worker worker) {
        if (!workerConnections.containsKey(worker.getId())) {
            WorkerClient workerClient = new WorkerClient(worker.getUrl());
            workerClient.start();
            workerConnections.put(worker.getId(), workerClient);
        }
        return workerConnections.get(worker.getId());
    }

    public synchronized void removeConnection(Worker worker) {
        workerConnections.remove(worker.getId());
    }
}
