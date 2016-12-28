package com.hermes.network.packet;

public class MessagePacket extends Packet {
    private String message;

    public MessagePacket(String message) {
        super(PacketType.MESSAGE);
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
