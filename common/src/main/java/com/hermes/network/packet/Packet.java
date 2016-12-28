package com.hermes.network.packet;

import java.io.Serializable;
import java.util.UUID;

public abstract class Packet implements Serializable {
    public final PacketType TYPE;
    public final String MESSAGE_ID;

    public Packet(PacketType type) {
        this.MESSAGE_ID = UUID.randomUUID().toString();
        this.TYPE = type;
    }
}
