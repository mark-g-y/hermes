package com.hermes;

import com.hermes.network.SocketServer;
import com.hermes.network.packet.AckPacket;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.packet.Packet;

import java.util.ArrayList;
import java.util.List;

public class MockWorker extends SocketServer {
    private List<String> receivedMessages;

    public MockWorker(int port) {
        super(port);
        receivedMessages = new ArrayList<>();
    }

    @Override
    protected void onClientConnected(HandlerThread thread) {
        // do nothing
    }

    @Override
    protected void onReceive(HandlerThread thread, Packet packet) {
        // mock worker will simply respond with an ack
        receivedMessages.add(((MessagePacket)packet).getMessage());
        AckPacket ackPacket = new AckPacket(packet.MESSAGE_ID);
        send(thread, ackPacket);
    }

    public List<String> getReceivedMessages() {
        return receivedMessages;
    }
}
