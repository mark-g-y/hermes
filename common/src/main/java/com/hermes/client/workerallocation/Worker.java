package com.hermes.client.workerallocation;

import java.io.Serializable;

public class Worker implements Serializable {
    private String id;
    private String url;

    public Worker(String id, String url) {
        this.id = id;
        this.url = url;
    }

    public String getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }
}
