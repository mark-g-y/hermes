package com.hermes.network.packet;

import com.hermes.client.ClientType;

public class ConsumerInitPacket extends InitPacket {
    private String groupName;

    public ConsumerInitPacket(String channelName, String groupName, long toleratedTimeout) {
        super(ClientType.CONSUMER, channelName, toleratedTimeout);
        this.groupName = groupName;
    }

    public ConsumerInitPacket(String channelName, String groupName) {
        super(ClientType.CONSUMER, channelName);
        this.groupName = groupName;
    }

    public String getGroupName() {
        return groupName;
    }
}
