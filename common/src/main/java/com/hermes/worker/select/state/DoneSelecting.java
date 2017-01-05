package com.hermes.worker.select.state;

import com.hermes.fsm.Context;
import com.hermes.fsm.State;

public class DoneSelecting implements State {
    public static final String NAME = "done_selecting";

    @Override
    public State execute(Context context) {
        return null;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
