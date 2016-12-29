package com.hermes.network;

import com.hermes.network.packet.AckPacket;
import com.hermes.network.packet.Packet;
import com.hermes.network.timeout.PacketTimeoutManager;
import com.hermes.network.timeout.TimeoutConfig;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class PacketSendClient extends SocketClient {
    private PacketTimeoutManager packetTimeoutManager;
    private Thread receiverThread;
    private PacketSendClientCallback callback;

    public PacketSendClient(String url, PacketSendClientCallback callback) {
        super(url);
        this.callback = callback;
        this.packetTimeoutManager = new PacketTimeoutManager();
    }

    @Override
    protected void run() {
        receiverThread = new Thread(() -> {
            try {
                Packet packet = readReply();
                while (packet != null) {
                    switch (packet.TYPE) {
                        case ACK:
                            packetTimeoutManager.messageReceived(((AckPacket) packet).ackMessageId);
                            break;
                        default:
                            System.out.println("Error - received unrecognized packet type " + packet.TYPE);
                    }
                    packet = readReply();
                }
            } catch (IOException e) {
                callback.onError(e);
            }
        });
        receiverThread.start();
    }

    public void send(Packet packet, CompletableFuture<Void> sendPacketFuture) {
        packetTimeoutManager.add(packet.MESSAGE_ID, TimeoutConfig.TIMEOUT, sendPacketFuture);
        try {
            send(packet);
        } catch (IOException e) {
            sendPacketFuture.completeExceptionally(e);
        }
    }

    public void stop() {
        packetTimeoutManager.stop();
        receiverThread.stop();
        shutdown();
    }
}