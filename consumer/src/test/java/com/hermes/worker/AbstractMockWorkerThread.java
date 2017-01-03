package com.hermes.worker;

import com.hermes.network.SocketServerHandlerThread;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.net.Socket;

public abstract class AbstractMockWorkerThread extends SocketServerHandlerThread {
    protected String id;
    protected ZooKeeper zk;

    public AbstractMockWorkerThread(String id, int port, Socket socket) {
        super(socket);
        this.id = id;
        this.zk = ZKManager.get();
        try {
            ZKUtility.createIgnoreExists(zk, ZKPaths.WORKERS + "/" + id, ("localhost:" + port).getBytes(),
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
        }
    }
}
