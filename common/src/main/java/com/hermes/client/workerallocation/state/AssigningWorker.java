package com.hermes.client.workerallocation.state;

import com.hermes.partition.AssignPartitionSocketClient;
import com.hermes.concurrency.DistributedGlobalLock;
import com.hermes.fsm.Context;
import com.hermes.fsm.State;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class AssigningWorker implements State {
    public static final String NAME = "assigning_worker";

    @Override
    public State execute(Context context) {
        String partition = (String)context.attrs.get("partition");
        ZooKeeper zk = ZKManager.get();
        try {
            boolean alreadyExists = ZKUtility.createIgnoreExists(zk, ZKPaths.ASSIGNING_WORKERS + "/" + partition, null,
                                                                 ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            if (alreadyExists) {
                // someone else is already assigning workers for this partition, so we shouldn't do it
                return context.states.getByName(GettingWorkers.NAME);
            }
            DistributedGlobalLock lock = new DistributedGlobalLock(zk);
            ZKUtility.createIgnoreExists(zk, ZKPaths.EDIT_PARTITIONS + "/" + partition, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            lock.lock(ZKPaths.EDIT_PARTITIONS + "/" + partition);
            List<String> workers = zk.getChildren(ZKPaths.WORKERS, null);
            // <TODO> use worker load to determine which worker to select
            int index = (int)(Math.random() * workers.size());
            String worker = workers.get(index);
            String url = new String(zk.getData(ZKPaths.WORKERS + "/" + worker, null, null));
            CompletableFuture future = new CompletableFuture();
            AssignPartitionSocketClient assignPartitionSocketClient = new AssignPartitionSocketClient(url, partition, future);
            assignPartitionSocketClient.start();
            future.get();
            assignPartitionSocketClient.shutdown();
            lock.unlock();
            zk.delete(ZKPaths.ASSIGNING_WORKERS + "/" + partition, -1);
        } catch (Exception e) {
        }
        return context.states.getByName(GettingWorkers.NAME);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
