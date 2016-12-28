package com.hermes.zookeeper;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZKManager {
    private static ZKManager zkManager;

    private ZooKeeper zk;

    public ZKManager(String zkUrl) {
        connectZooKeeper(zkUrl);
    }

    public ZooKeeper getZooKeeper() {
        return zk;
    }

    public void shutdown() throws InterruptedException {
        zk.close();
    }

    private void connectZooKeeper(final String zookeeperUrl) {
        try {
            zk = new ZooKeeper(zookeeperUrl, 30000, new ReconnectWatcher(() -> connectZooKeeper(zookeeperUrl)));
        } catch (IOException e) {
            e.printStackTrace();
            connectZooKeeper(zookeeperUrl);
        }
    }

    public static void init(String zkUrl) {
        if (zkManager == null) {
            zkManager = new ZKManager(zkUrl);
        } else {
            System.out.println("Warning - ZKManager.init() already invoked");
        }
    }

    public static ZooKeeper get() {
        if (zkManager == null) {
            throw new RuntimeException("Need to call init() before using");
        }
        return zkManager.getZooKeeper();
    }
}
