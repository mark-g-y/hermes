package com.hermes.worker.select.state;

import com.hermes.fsm.Context;
import com.hermes.fsm.State;
import com.hermes.worker.WorkerManager;

public class AllocatingNewWorkers implements State {
    public static final String NAME = "allocating_new_workers";

    @Override
    public State execute(Context context) {
        String partition = (String)context.attrs.get("partition");
        int numToSelect = (int)context.attrs.get("num_to_select");
        WorkerManager.allocateWorkersForPartition(partition, numToSelect);
        return context.states.getByName(Selecting.NAME);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
