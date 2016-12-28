package com.hermes.network.packet;

public class AckPacket extends Packet {
    public String ackMessageId;

    public AckPacket(String ackMessageId) {
        super(PacketType.ACK);
        this.ackMessageId = ackMessageId;
    }
}
