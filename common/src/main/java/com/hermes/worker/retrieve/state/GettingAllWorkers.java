package com.hermes.worker.retrieve.state;

import com.hermes.fsm.Context;
import com.hermes.fsm.State;
import com.hermes.worker.WorkerManager;
import com.hermes.worker.metadata.Worker;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class GettingAllWorkers implements State {
    public static final String NAME = "assigning_worker";
    private ZooKeeper zk;

    public GettingAllWorkers() {
        zk = ZKManager.get();
    }

    @Override
    public State execute(Context context) {
        try {
            List<String> workerIds = zk.getChildren(ZKPaths.WORKERS, null);
            List<Worker> workers = WorkerManager.getWorkers(zk, workerIds);
            context.attrs.put("workers", workers);
        } catch (Exception e) {
            e.printStackTrace();
            return context.states.getByName(Disconnected.NAME);
        }
        return context.states.getByName(FinishedGettingAllWorkers.NAME);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
