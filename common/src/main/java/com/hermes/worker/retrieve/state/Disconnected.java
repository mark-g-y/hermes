package com.hermes.worker.retrieve.state;

import com.hermes.fsm.Context;
import com.hermes.fsm.State;

public class Disconnected implements State {
    public static final String NAME = "disconnected";

    @Override
    public State execute(Context context) {
        return context.states.getByName(GettingAllWorkers.NAME);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
