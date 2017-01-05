package com.hermes;

import com.hermes.partition.PartitionConfigs;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class Initializer {
    private ZooKeeper zk;

    public Initializer(String zkUrl) {
        ZKManager.init(zkUrl);
        this.zk = ZKManager.get();
    }

    public void run() {
        try {
            ZKUtility.createIgnoreExists(zk, ZKPaths.ROOT, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            ZKUtility.createIgnoreExists(zk, ZKPaths.WORKERS, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            ZKUtility.createIgnoreExists(zk, ZKPaths.WORKER_LOADS, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            ZKUtility.createIgnoreExists(zk, ZKPaths.ASSIGNING_WORKERS, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            ZKUtility.createIgnoreExists(zk, ZKPaths.EDIT_PARTITIONS, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            for (int i = 0; i < PartitionConfigs.NUM_PARTITIONS; i++) {
                ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + Integer.toString(i), null,
                                             ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                ZKUtility.createIgnoreExists(zk, ZKPaths.EDIT_PARTITIONS + "/" + Integer.toString(i), null,
                                             ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
