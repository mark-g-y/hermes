package com.hermes.client.workerassignment.state;

import com.hermes.fsm.Context;
import com.hermes.fsm.State;
import com.hermes.worker.WorkerManager;

public class AllocatingWorker implements State {
    public static final String NAME = "allocating_worker";

    @Override
    public State execute(Context context) {
        String partition = (String)context.attrs.get("partition");
        WorkerManager.allocateWorkersForPartition(partition, 1);
        return context.states.getByName(GettingWorkers.NAME);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
