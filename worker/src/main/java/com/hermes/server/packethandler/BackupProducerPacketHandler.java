package com.hermes.server.packethandler;

import com.hermes.message.ChannelMessageQueues;
import com.hermes.message.Message;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.timeout.PacketTimeoutManager;
import com.hermes.server.ServerToClientSender;
import com.hermes.worker.metadata.Worker;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class BackupProducerPacketHandler extends PacketHandler {
    private String channelName;
    private List<Worker> backups;
    private long ackTimeout;
    private ChannelMessageQueues channelMessageQueues;

    public BackupProducerPacketHandler(String workerId, String channelName, List<Worker> backups, long ackTimeout,
                                       ChannelMessageQueues channelMessageQueues,
                                       PacketTimeoutManager packetTimeoutManager, ServerToClientSender sender) {
        super(workerId, packetTimeoutManager, sender);
        this.channelName = channelName;
        this.backups = backups;
        this.ackTimeout = ackTimeout;
        this.channelMessageQueues = channelMessageQueues;
    }

    @Override
    public void onMessage(MessagePacket packet) {
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        ackFuture.exceptionally((throwable) -> {
            channelMessageQueues.add(channelName, new Message(packet, backups));
            return null;
        });
        packetTimeoutManager.add(packet.MESSAGE_ID, ackTimeout, ackFuture);
        sendAck(packet.MESSAGE_ID);
    }

    @Override
    public void stop() {
    }
}
