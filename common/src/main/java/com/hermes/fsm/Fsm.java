package com.hermes.fsm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class Fsm {
    private List<State> states;
    private State currentState;
    private Context context;
    private HashMap<String, List<StateTrigger>> triggers;
    private boolean started;

    public Fsm() {
        states = new ArrayList<>();
        context = new Context();
        triggers = new HashMap<>();
        started = false;
    }

    public void addState(State state) {
        states.add(state);
    }

    public void addStates(Collection<State> states) {
        this.states.addAll(states);
    }

    public synchronized void addTrigger(String stateName, StateTrigger trigger) {
        if (!triggers.containsKey(stateName)) {
            triggers.put(stateName, new ArrayList<>());
        }
        triggers.get(stateName).add(trigger);
    }

    public Context getContext() {
        return context;
    }

    public synchronized void run(State initialState) {
        if (started) {
            throw new RuntimeException("FSM has already been started");
        }
        started = true;

        context.states = new States(states);
        currentState = initialState;
        while (currentState != null) {
            currentState = currentState.execute(context);
            if (currentState != null && triggers.containsKey(currentState.getName())) {
                for (StateTrigger trigger : triggers.get(currentState.getName())) {
                    trigger.onTrigger(context);
                }
            }
        }
    }
}
