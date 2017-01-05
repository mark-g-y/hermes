package com.hermes;

import com.hermes.network.SocketServer;
import com.hermes.network.SocketServerHandlerThread;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import junit.framework.Assert;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class MockWorker extends SocketServer {
    private String id;
    private List<String> receivedMessages;
    private ZooKeeper zk;

    public MockWorker(String id, int port) {
        super(port);
        this.id = id;
        this.receivedMessages = new ArrayList<>();
        this.zk = ZKManager.get();

        try {
            ZKUtility.createIgnoreExists(zk, ZKPaths.WORKERS + "/" + id, ("localhost:" + port).getBytes(),
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            ZKUtility.createIgnoreExists(zk, ZKPaths.WORKER_LOADS + "/" + id, "0".getBytes(),
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    public List<String> getReceivedMessages() {
        return receivedMessages;
    }

    @Override
    protected SocketServerHandlerThread buildHandlerThread(Socket socket) {
        return new MockWorkerThread(socket, receivedMessages);
    }
}
