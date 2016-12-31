package com.hermes.worker;

import com.hermes.network.SocketServer;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public abstract class AbstractMockWorker extends SocketServer {
    protected String id;
    protected ZooKeeper zk;

    public AbstractMockWorker(String id, int port) throws Exception {
        super(port);
        this.id = id;
        this.zk = ZKManager.get();
        ZKUtility.createIgnoreExists(zk, ZKPaths.WORKERS + "/" + id, ("localhost:" + port).getBytes(),
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    @Override
    public void stop() {
        super.stop();
        try {
            zk.delete(ZKPaths.WORKERS + "/" + id, -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getId() {
        return id;
    }
}
