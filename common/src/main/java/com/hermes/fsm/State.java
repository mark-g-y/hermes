package com.hermes.fsm;

public interface State {
    State execute(Context context);
    String getName();
}
