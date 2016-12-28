package com.hermes.fsm;

import java.util.concurrent.ConcurrentHashMap;

public class Context {
    public States states;
    public ConcurrentHashMap<String, Object> attrs = new ConcurrentHashMap<>();
}
