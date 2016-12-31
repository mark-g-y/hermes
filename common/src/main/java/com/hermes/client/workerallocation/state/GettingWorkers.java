package com.hermes.client.workerallocation.state;

import com.hermes.client.workerallocation.Worker;
import com.hermes.fsm.Context;
import com.hermes.fsm.State;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
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
                return context.states.getByName(AssigningWorker.NAME);
            }
            List<Worker> workers = getWorkers(workerIds);
            context.attrs.put("workers", workers);
            return context.states.getByName(FinishedAllocatingWorkers.NAME);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return context.states.getByName(GettingWorkers.NAME);
    }

    private List<Worker> getWorkers(List<String> workerIds) {
        List<Worker> workers = new ArrayList<>();
        for (String workerId : workerIds) {
            try {
                String url = new String(zk.getData(ZKPaths.WORKERS + "/" + workerId, null, null));
                workers.add(new Worker(workerId, url));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return workers;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
