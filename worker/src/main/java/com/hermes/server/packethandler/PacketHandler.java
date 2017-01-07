package com.hermes.server.packethandler;

import com.hermes.network.packet.AckPacket;
import com.hermes.network.packet.AssignPartitionPacket;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.timeout.PacketTimeoutManager;
import com.hermes.server.ServerToClientSender;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public abstract class PacketHandler {
    protected String workerId;
    protected PacketTimeoutManager packetTimeoutManager;
    protected ServerToClientSender sender;

    public PacketHandler(String workerId, PacketTimeoutManager packetTimeoutManager, ServerToClientSender sender) {
        this.workerId = workerId;
        this.packetTimeoutManager = packetTimeoutManager;
        this.sender = sender;
    }

    public void onAssignPartition(AssignPartitionPacket packet) {
        ZooKeeper zk = ZKManager.get();
        try {
            ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + packet.getPartition() + "/" + workerId, null,
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            sender.send(new AckPacket(packet.MESSAGE_ID));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void onAck(AckPacket packet) {
        packetTimeoutManager.messageReceived(packet.ackMessageId);
    }

    protected void sendAck(String messageId) {
        try {
            sender.send(new AckPacket(messageId));
        } catch (IOException e) {
            // failures will be handled by backups
            e.printStackTrace();
        }
    }

    public abstract void onMessage(MessagePacket packet);
    public abstract void stop();
}
