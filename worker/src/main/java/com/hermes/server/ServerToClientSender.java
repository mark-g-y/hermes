package com.hermes.server;

import com.hermes.network.packet.Packet;

import java.io.IOException;

public interface ServerToClientSender {
    void send(Packet packet) throws IOException;
}
