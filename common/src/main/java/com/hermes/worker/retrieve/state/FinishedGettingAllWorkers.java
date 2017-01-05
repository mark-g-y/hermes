package com.hermes.worker.retrieve.state;

import com.hermes.fsm.Context;
import com.hermes.fsm.State;

public class FinishedGettingAllWorkers implements State {
    public static final String NAME = "finished_getting_all_workers";

    @Override
    public State execute(Context context) {
        return null;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
