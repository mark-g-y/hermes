package com.hermes.network.packet;

import com.hermes.client.ClientType;
import com.hermes.client.workerallocation.Worker;
import com.hermes.network.timeout.TimeoutConfig;

import java.util.Collections;
import java.util.List;

public class InitPacket extends Packet {
    private ClientType clientType;
    private String channelName;
    private List<Worker> backups;
    private long toleratedTimeout;

    public InitPacket(ClientType clientType, String channelName, List<Worker> backups, long toleratedTimeout) {
        super(PacketType.INIT);
        this.clientType = clientType;
        this.channelName = channelName;
        this.backups = backups;
        this.toleratedTimeout = toleratedTimeout;
    }

    public InitPacket(ClientType clientType, String channelName) {
        this(clientType, channelName, Collections.emptyList(), TimeoutConfig.TIMEOUT);
    }

    public ClientType getClientType() {
        return clientType;
    }

    public List<Worker> getBackups() {
        return backups;
    }

    public String getChannelName() {
        return channelName;
    }

    public long getToleratedTimeout() {
        return toleratedTimeout;
    }
}
