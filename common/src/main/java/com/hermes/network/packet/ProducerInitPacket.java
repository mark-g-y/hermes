package com.hermes.network.packet;

import com.hermes.client.ClientType;
import com.hermes.network.timeout.TimeoutConfig;
import com.hermes.worker.metadata.Worker;

import java.util.Collections;
import java.util.List;

public class ProducerInitPacket extends InitPacket {
    private List<Worker> backups;

    public ProducerInitPacket(ClientType clientType, String channelName, List<Worker> backups, long toleratedTimeout) {
        super(clientType, channelName, toleratedTimeout);
        this.backups = backups;
    }

    public ProducerInitPacket(ClientType clientType, String channelName, List<Worker> backups) {
        this(clientType, channelName, backups, TimeoutConfig.TIMEOUT);
    }

    public ProducerInitPacket(ClientType clientType, String channelName) {
        this(clientType, channelName, Collections.emptyList());
    }

    public List<Worker> getBackups() {
        return backups;
    }
}
