package com.hermes.server.packethandler;

import com.hermes.network.packet.MessagePacket;
import com.hermes.network.timeout.PacketTimeoutManager;
import com.hermes.server.ServerToClientSender;

/**
 * This packet handler is the default handler that takes care of generic responsibilities
 */
public class DefaultPacketHandler extends PacketHandler {
    public DefaultPacketHandler(String workerId, PacketTimeoutManager packetTimeoutManager, ServerToClientSender sender) {
        super(workerId, packetTimeoutManager, sender);
    }

    @Override
    public void onMessage(MessagePacket packet) {
    }

    @Override
    public void stop() {
    }
}
