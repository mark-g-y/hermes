package com.hermes.worker;

import com.hermes.network.packet.AckPacket;
import com.hermes.network.packet.Packet;
import com.hermes.network.packet.PacketType;
import com.hermes.network.timeout.PacketTimeoutManager;

import java.net.Socket;

public class MessageBroadcastWorkerHandler extends AbstractMockWorkerHandler {
    private PacketTimeoutManager packetTimeoutManager;

    public MessageBroadcastWorkerHandler(String id, int port, Socket socket, PacketTimeoutManager packetTimeoutManager) {
        super(id, port, socket);
        this.packetTimeoutManager = packetTimeoutManager;
    }

    @Override
    protected void onClientConnected() {
    }

    @Override
    protected void onReceive(Packet packet) {
        PacketType packetType = packet.TYPE;
        switch (packetType) {
            case ACK:
                packetTimeoutManager.messageReceived(((AckPacket)packet).ackMessageId);
                break;
            default:
                System.out.println("Unused packet type - " + packetType);
        }
    }

    @Override
    protected void onDisconnect() {
    }
}
