package com.hermes.worker;

import com.hermes.network.packet.AckPacket;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.packet.Packet;
import com.hermes.network.packet.PacketType;
import com.hermes.network.timeout.PacketTimeoutManager;
import com.hermes.network.timeout.TimeoutConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MessageBroadcastWorker extends AbstractMockWorker {
    private PacketTimeoutManager packetTimeoutManager;

    public MessageBroadcastWorker(String id, int port) throws Exception {
        super(id, port);
        this.packetTimeoutManager = new PacketTimeoutManager();
    }

    @Override
    protected void onClientConnected(HandlerThread thread) {
    }

    @Override
    protected void onReceive(HandlerThread thread, Packet packet) {
        PacketType packetType = packet.TYPE;
        switch (packetType) {
            case ACK:
                packetTimeoutManager.messageReceived(((AckPacket)packet).ackMessageId);
                break;
            default:
                throw new RuntimeException("Unrecognized packet type - " + packetType);
        }
    }

    public List<CompletableFuture<Void>> broadcastMessage(String message) {
        List<CompletableFuture<Void>> messageAckFutures = new ArrayList<>();
        for (HandlerThread thread : threads) {
            MessagePacket packet = new MessagePacket(message);
            CompletableFuture<Void> messageAckFuture = new CompletableFuture<>();
            messageAckFutures.add(messageAckFuture);
            packetTimeoutManager.add(packet.MESSAGE_ID, TimeoutConfig.TIMEOUT, messageAckFuture);
            send(thread, packet);
        }
        return messageAckFutures;
    }
}
