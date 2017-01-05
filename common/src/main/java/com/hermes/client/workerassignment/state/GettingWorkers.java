package com.hermes.client.workerassignment.state;

import com.hermes.worker.WorkerManager;
import com.hermes.worker.metadata.Worker;
import com.hermes.fsm.Context;
import com.hermes.fsm.State;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class GettingWorkers implements State {
    public static final String NAME = "getting_workers";

    private Watcher workerWatcher;
    private ZooKeeper zk;

    public GettingWorkers(Watcher workerWatcher) {
        this.workerWatcher = workerWatcher;
        this.zk = ZKManager.get();
    }

    @Override
    public State execute(Context context) {
        String partition = (String)context.attrs.get("partition");
        try {
            List<String> workerIds = zk.getChildren(ZKPaths.PARTITIONS + "/" + partition, workerWatcher);
            if (workerIds.size() == 0) {
                return context.states.getByName(AllocatingWorker.NAME);
            }
            List<Worker> workers = WorkerManager.getWorkers(zk, workerIds);
            context.attrs.put("workers", workers);
            return context.states.getByName(FinishedAssigningWorkers.NAME);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return context.states.getByName(GettingWorkers.NAME);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
