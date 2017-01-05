package com.hermes.client.workerassignment.state;

import com.hermes.fsm.Context;
import com.hermes.fsm.State;

public class FinishedAssigningWorkers implements State {
    public static final String NAME = "finished_allocating_workers";

    @Override
    public State execute(Context context) {
        return null;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
