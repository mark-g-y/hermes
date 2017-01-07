package com.hermes;

import com.hermes.network.SocketServerHandler;
import com.hermes.network.packet.AckPacket;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.packet.Packet;
import com.hermes.network.packet.PacketType;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

public class MockWorkerHandler extends SocketServerHandler {
    private List<String> receivedMessages; // no need for thread safety in this test

    public MockWorkerHandler(Socket socket, List<String> receivedMessages) {
        super(socket);
        this.receivedMessages = receivedMessages;
    }

    @Override
    protected void onClientConnected() {
        // do nothing
    }

    @Override
    protected void onReceive(Packet packet) {
        // mock worker will simply respond with an ack
        PacketType packetType = packet.TYPE;
        if (packetType == PacketType.MESSAGE) {
            receivedMessages.add(((MessagePacket) packet).getMessage());
            AckPacket ackPacket = new AckPacket(packet.MESSAGE_ID);
            try {
                send(ackPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void onDisconnect() {
        // do nothing
    }
}
