package com.hermes.fsm;

import java.util.HashMap;
import java.util.List;

public class States {
    private HashMap<String, State> states;

    public States() {
        states = new HashMap<>();
    }

    public States(List<State> stateList) {
        this();
        for (State state : stateList) {
            states.put(state.getName(), state);
        }
    }

    public State getByName(String name) {
        return states.get(name);
    }
}
