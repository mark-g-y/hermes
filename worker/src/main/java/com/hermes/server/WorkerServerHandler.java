package com.hermes.server;

import com.hermes.client.ClientType;
import com.hermes.connection.ConsumerConnectionsManager;
import com.hermes.connection.ProducerConnectionsManager;
import com.hermes.message.MessageQueues;
import com.hermes.server.packethandler.*;
import com.hermes.connection.WorkerToWorkerConnectionsManager;
import com.hermes.network.SocketServerHandler;
import com.hermes.network.packet.*;
import com.hermes.network.timeout.PacketTimeoutManager;

import java.net.Socket;

public class WorkerServerHandler extends SocketServerHandler implements ServerToClientSender {
    private PacketHandler packetHandler;
    private String id;
    private String channelName;
    private String groupName;
    private MessageQueues messageQueues;
    private PacketTimeoutManager packetTimeoutManager;
    private ProducerConnectionsManager producerConnectionsManager;
    private ConsumerConnectionsManager consumerConnectionsManager;
    private WorkerToWorkerConnectionsManager workerToWorkerConnectionsManager;

    public WorkerServerHandler(Socket socket,
                               String id,
                               MessageQueues messageQueues,
                               PacketTimeoutManager packetTimeoutManager,
                               ProducerConnectionsManager producerConnectionsManager,
                               ConsumerConnectionsManager consumerConnectionsManager,
                               WorkerToWorkerConnectionsManager workerToWorkerConnectionsManager) {
        super(socket);
        this.packetHandler = new DefaultPacketHandler(id, packetTimeoutManager, this);
        this.id = id;
        this.messageQueues = messageQueues;
        this.packetTimeoutManager = packetTimeoutManager;
        this.producerConnectionsManager = producerConnectionsManager;
        this.consumerConnectionsManager = consumerConnectionsManager;
        this.workerToWorkerConnectionsManager = workerToWorkerConnectionsManager;
    }

    @Override
    protected void onClientConnected() {
        // do nothing, no need to send anything
    }

    @Override
    protected void onReceive(Packet packet) {
        // handle received packet
        switch (packet.TYPE) {
            case ASSIGN_PARTITION:
                packetHandler.onAssignPartition((AssignPartitionPacket)packet);
                break;
            case INIT:
                handleInit((InitPacket)packet);
                break;
            case MESSAGE:
                packetHandler.onMessage((MessagePacket)packet);
                break;
            case ACK:
                packetHandler.onAck((AckPacket)packet);
                break;
            default:
                System.out.println("Error - received unrecognized packet type " + packet.TYPE);
        }
    }

    @Override
    protected void onDisconnect() {
        packetHandler.stop();
        removeFromWorkerToClientConnections();
    }

    private void handleInit(InitPacket packet) {
        packetHandler.stop();
        removeFromWorkerToClientConnections();

        channelName = packet.getChannelName();

        if (packet.getClientType() == ClientType.CONSUMER) {
            groupName = ((ConsumerInitPacket)packet).getGroupName();
            consumerConnectionsManager.add(channelName, groupName, this);
            packetHandler = new ConsumerPacketHandler(id, messageQueues.getQueueCreateIfNotExists(channelName, groupName),
                                                      packetTimeoutManager, workerToWorkerConnectionsManager, this);
        } else if (packet.getClientType() == ClientType.PRODUCER_MAIN) {
            producerConnectionsManager.add(channelName, this);
            packetHandler = new MainProducerPacketHandler(id, channelName, ((ProducerInitPacket)packet).getBackups(),
                                                          messageQueues, packetTimeoutManager, this);
        } else if (packet.getClientType() == ClientType.PRODUCER_BACKUP) {
            producerConnectionsManager.add(channelName, this);
            packetHandler = new BackupProducerPacketHandler(id, channelName, ((ProducerInitPacket)packet).getBackups(),
                                                            packet.getToleratedTimeout(), messageQueues,
                                                            packetTimeoutManager, this);
        }
    }

    private void removeFromWorkerToClientConnections() {
        producerConnectionsManager.remove(channelName, this);
        consumerConnectionsManager.remove(channelName, groupName, this);
        messageQueues.removeUnused(channelName, groupName, consumerConnectionsManager);
    }
}
