package com.hermes.worker;

import com.hermes.network.SocketServer;
import com.hermes.network.SocketServerHandlerThread;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionCounterSocketServer extends SocketServer {
    private String id;
    private int port;
    private AtomicInteger numPreviousAndCurrentConnections;

    public ConnectionCounterSocketServer(String id, int port) {
        super(port);
        this.id = id;
        this.port = port;
        this.numPreviousAndCurrentConnections = new AtomicInteger(0);
    }

    @Override
    public void start() {
        try {
            ZKUtility.createIgnoreExists(ZKManager.get(), ZKPaths.WORKERS + "/" + id, ("localhost:" + port).getBytes(),
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.start();
    }

    public String getId() {
        return id;
    }

    public int getNumPreviousAndCurrentConnections() {
        return numPreviousAndCurrentConnections.get();
    }

    @Override
    protected SocketServerHandlerThread buildHandlerThread(Socket socket) {
        return new ConnectionCounterWorkerThread(id, port, socket, numPreviousAndCurrentConnections);
    }
}
