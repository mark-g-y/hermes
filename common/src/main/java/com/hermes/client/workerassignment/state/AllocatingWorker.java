package com.hermes.client.workerassignment.state;

import com.hermes.fsm.Context;
import com.hermes.fsm.State;
import com.hermes.worker.WorkerManager;
import com.hermes.worker.allocation.WorkerAllocator;
import com.hermes.worker.metadata.Worker;

import java.util.List;

public class AllocatingWorker implements State {
    public static final String NAME = "assigning_worker";

    @Override
    public State execute(Context context) {
        String partition = (String)context.attrs.get("partition");
        try {
            List<Worker> workers = WorkerManager.getAllWorkers();
            if (workers.size() < 1) {
                return context.states.getByName(GettingWorkers.NAME);
            }
            Worker worker = workers.get(0);
            new WorkerAllocator().allocateToPartition(worker, partition);
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
