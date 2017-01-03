package com.hermes.network.packet;

import com.hermes.client.workerallocation.Worker;

import java.util.Collections;
import java.util.List;

public class InitPacket extends Packet {
    private ClientType clientType;
    private String channelName;
    private List<Worker> backups;

    public InitPacket(ClientType clientType, String channelName, List<Worker> backups) {
        super(PacketType.INIT);
        this.clientType = clientType;
        this.channelName = channelName;
        this.backups = backups;
    }

    public InitPacket(ClientType clientType, String channelName) {
        this(clientType, channelName, Collections.emptyList());
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

    public enum ClientType {
        CONSUMER, PRODUCER
    }
}
