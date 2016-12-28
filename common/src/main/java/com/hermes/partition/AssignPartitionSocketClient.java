package com.hermes.partition;

import com.hermes.network.SocketClient;
import com.hermes.network.packet.AckPacket;
import com.hermes.network.packet.AssignPartitionPacket;
import com.hermes.network.timeout.PacketTimeoutManager;
import com.hermes.network.timeout.TimeoutConfig;

import java.util.concurrent.CompletableFuture;

public class AssignPartitionSocketClient extends SocketClient {
    private PacketTimeoutManager packetTimeoutManager;
    private String partition;
    private CompletableFuture future;

    public AssignPartitionSocketClient(String url, String partition, CompletableFuture future) {
        super(url);
        this.partition = partition;
        this.future = future;
        this.packetTimeoutManager = new PacketTimeoutManager();
    }

    @Override
    public void run() {
        try {
            AssignPartitionPacket assignPartitionPacket = new AssignPartitionPacket(partition);
            packetTimeoutManager.add(assignPartitionPacket.MESSAGE_ID, TimeoutConfig.TIMEOUT, future);
            send(assignPartitionPacket);

            AckPacket ackPacket = (AckPacket) readReply();
            packetTimeoutManager.messageReceived(ackPacket.ackMessageId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        packetTimeoutManager.stop();
    }
}
