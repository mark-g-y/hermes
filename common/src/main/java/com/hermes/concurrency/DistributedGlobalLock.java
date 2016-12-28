package com.hermes.concurrency;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.Collections;
import java.util.List;

public class DistributedGlobalLock {
    private ZooKeeper zk;
    private String lockPath;

    public DistributedGlobalLock(ZooKeeper zk) throws Exception {
        this.zk = zk;
    }

    public void lock(String basePath) throws Exception {
        lockPath = zk.create(basePath + "/lock", null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                             CreateMode.EPHEMERAL_SEQUENTIAL);
        final Object lock = new Object();
        synchronized(lock) {
            while(true) {
                List<String> nodes = zk.getChildren(basePath, (event) -> {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                });
                Collections.sort(nodes);
                if (nodes.size() == 0 || lockPath.endsWith(nodes.get(0))) {
                    return;
                } else {
                    lock.wait();
                }
            }
        }
    }

    public void unlock() throws Exception {
        zk.delete(lockPath, -1);
    }
}
