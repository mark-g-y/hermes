package com.hermes;

import com.hermes.network.SocketServer;
import com.hermes.server.WorkerSocketServer;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.UUID;

public class Worker {
    private String id;
    private String host;
    private int port;
    private String url;

    private SocketServer workerSocketServer;
    private ZooKeeper zk;

    public Worker(String host, int port) {
        this(UUID.randomUUID().toString(), host, port);
    }

    public Worker(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.url = host + ":" + port;

        this.zk = ZKManager.get();
    }

    public void start() {
        createWorkerInZooKeeper();
        workerSocketServer = new WorkerSocketServer(id, port);
        workerSocketServer.start();
    }

    public void stop() {
        workerSocketServer.stop();
    }

    private void createWorkerInZooKeeper() {
        try {
            zk.create(ZKPaths.WORKERS + "/" + id, url.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            System.out.println("Error - worker ID already exists");
        }
    }
}
