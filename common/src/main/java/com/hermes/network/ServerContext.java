package com.hermes.network;

import java.util.HashMap;

public class ServerContext {
    private HashMap<String, Object> attributes;

    public ServerContext() {
        attributes = new HashMap<>();
    }

    public void put(String key, Object value) {
        attributes.put(key, value);
    }

    public Object get(String key) {
        return attributes.get(key);
    }

    public boolean contains(String key) {
        return attributes.containsKey(key);
    }
}
