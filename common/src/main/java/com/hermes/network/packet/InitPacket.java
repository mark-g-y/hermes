package com.hermes.network.packet;

import com.hermes.client.ClientType;
import com.hermes.network.timeout.TimeoutConfig;

public abstract class InitPacket extends Packet {
    protected ClientType clientType;
    protected String channelName;
    protected long toleratedTimeout;

    public InitPacket(ClientType clientType, String channelName, long toleratedTimeout) {
        super(PacketType.INIT);
        this.clientType = clientType;
        this.channelName = channelName;
        this.toleratedTimeout = toleratedTimeout;
    }

    public InitPacket(ClientType clientType, String channelName) {
        this(clientType, channelName, TimeoutConfig.TIMEOUT);
    }

    public ClientType getClientType() {
        return clientType;
    }

    public String getChannelName() {
        return channelName;
    }

    public long getToleratedTimeout() {
        return toleratedTimeout;
    }
}
