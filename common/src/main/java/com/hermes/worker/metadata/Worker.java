package com.hermes.worker.metadata;

import java.io.Serializable;

public class Worker implements Comparable<Worker>, Serializable {
    private String id;
    private String url;
    private double load;

    public Worker(String id, String url, double load) {
        this.id = id;
        this.url = url;
        this.load = load;
    }

    public String getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }

    public double getLoad() {
        return load;
    }

    @Override
    public int compareTo(Worker worker) {
        // need manual comparison instead of returning difference in-case double is truncated during integer cast
        // not a current issue since load is between 0-100, but in the future might change
        if (load < worker.getLoad()) {
            return -1;
        } else if (load > worker.getLoad()) {
            return 1;
        } else {
            return 0;
        }
    }
}
