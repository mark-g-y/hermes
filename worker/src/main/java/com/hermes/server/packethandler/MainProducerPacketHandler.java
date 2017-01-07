package com.hermes.server.packethandler;

import com.hermes.message.Message;
import com.hermes.message.MessageQueues;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.timeout.PacketTimeoutManager;
import com.hermes.server.ServerToClientSender;
import com.hermes.worker.metadata.Worker;

import java.util.List;

public class MainProducerPacketHandler extends PacketHandler {
    private String channelName;
    private List<Worker> backups;
    private MessageQueues messageQueues;

    public MainProducerPacketHandler(String workerId, String channelName, List<Worker> backups,
                                     MessageQueues messageQueues, PacketTimeoutManager packetTimeoutManager,
                                     ServerToClientSender sender) {
        super(workerId, packetTimeoutManager, sender);
        this.channelName = channelName;
        this.backups = backups;
        this.messageQueues = messageQueues;
    }

    @Override
    public void onMessage(MessagePacket packet) {
        messageQueues.add(channelName, new Message(packet, backups));
        sendAck(packet.MESSAGE_ID);
    }

    @Override
    public void stop() {
    }
}
