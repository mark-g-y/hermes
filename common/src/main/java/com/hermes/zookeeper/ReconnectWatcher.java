package com.hermes.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ReconnectWatcher implements Watcher {

    private Runnable reconnecter;

    public ReconnectWatcher(Runnable reconnecter) {
        this.reconnecter = reconnecter;
    }

    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.Disconnected) {
            reconnecter.run();
        }
    }
}
