package com.hermes.worker.allocation;

import com.hermes.concurrency.DistributedGlobalLock;
import com.hermes.network.timeout.TimeoutConfig;
import com.hermes.partition.AssignPartitionSocketClient;
import com.hermes.worker.metadata.Worker;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class WorkerAllocator {
    private ZooKeeper zk;

    public WorkerAllocator() {
        this.zk = ZKManager.get();
    }

    public boolean allocateToPartition(Worker worker, String partition) {
        try {
            boolean alreadyExists = ZKUtility.createIgnoreExists(zk, ZKPaths.ASSIGNING_WORKERS + "/" + partition, null,
                                                                 ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            if (alreadyExists) {
                // someone else is already assigning workers for this partition, so we shouldn't do it
                return true;
            }
        } catch (Exception e) {
        }
        boolean noErrors = false;
        DistributedGlobalLock lock = new DistributedGlobalLock(zk);
        try {
            lock.lock(ZKPaths.EDIT_PARTITIONS + "/" + partition);
            CompletableFuture<Void> future = new CompletableFuture<>();
            AssignPartitionSocketClient assignPartitionSocketClient = new AssignPartitionSocketClient(worker.getUrl(),
                                                                                                      partition, future);
            assignPartitionSocketClient.start();
            future.get(TimeoutConfig.TIMEOUT, TimeUnit.MILLISECONDS);
            assignPartitionSocketClient.shutdown();
            noErrors = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            lock.unlock();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            zk.delete(ZKPaths.ASSIGNING_WORKERS + "/" + partition, -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return noErrors;
    }
}
