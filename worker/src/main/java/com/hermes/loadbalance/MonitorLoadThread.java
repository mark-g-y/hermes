package com.hermes.loadbalance;

import com.hermes.os.MemoryUsage;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class MonitorLoadThread extends Thread {
    private static final long POLL_INTERVAL = 10000;
    private static final double MAX_LOAD = 80;

    private String workerId;
    private LoadRebalancer rebalancer;

    private ZooKeeper zk;

    public MonitorLoadThread(String workerId, LoadRebalancer rebalancer) {
        this.workerId = workerId;
        this.rebalancer = rebalancer;
        this.zk = ZKManager.get();
    }

    @Override
    public void run() {
        try {
            while (true) {
                double memoryLoad = MemoryUsage.getPercentage();
                updateMemory(memoryLoad);
                if (memoryLoad > MAX_LOAD) {
                    rebalancer.rebalanceWorkers();
                }
                Thread.sleep(POLL_INTERVAL);
            }
        } catch (InterruptedException e) {
        }
    }

    private void updateMemory(double memory) {
        String memoryLoad = Double.toString(memory);
        try {
            ZKUtility.createIgnoreExists(zk, ZKPaths.WORKER_LOADS + "/" + workerId, memoryLoad.getBytes(),
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zk.setData(ZKPaths.WORKER_LOADS + "/" + workerId, memoryLoad.getBytes(), -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
