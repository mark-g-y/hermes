package com.hermes.server;

import com.hermes.client.ClientType;
import com.hermes.server.packethandler.*;
import com.hermes.connection.ChannelClientConnectionsManager;
import com.hermes.connection.WorkerToWorkerConnectionsManager;
import com.hermes.message.ChannelMessageQueues;
import com.hermes.network.SocketServerHandlerThread;
import com.hermes.network.packet.*;
import com.hermes.network.timeout.PacketTimeoutManager;

import java.net.Socket;

public class WorkerServerHandlerThread extends SocketServerHandlerThread implements ServerToClientSender {
    private PacketHandler packetHandler;
    private String id;
    private String channelName;
    private ChannelMessageQueues channelMessageQueues;
    private PacketTimeoutManager packetTimeoutManager;
    private ChannelClientConnectionsManager producerConnectionsManager;
    private ChannelClientConnectionsManager consumerConnectionsManager;
    private WorkerToWorkerConnectionsManager workerToWorkerConnectionsManager;

    public WorkerServerHandlerThread(Socket socket,
                                     String id,
                                     ChannelMessageQueues channelMessageQueues,
                                     PacketTimeoutManager packetTimeoutManager,
                                     ChannelClientConnectionsManager producerConnectionsManager,
                                     ChannelClientConnectionsManager consumerConnectionsManager,
                                     WorkerToWorkerConnectionsManager workerToWorkerConnectionsManager) {
        super(socket);
        this.packetHandler = new DefaultPacketHandler(id, packetTimeoutManager, this);
        this.id = id;
        this.channelMessageQueues = channelMessageQueues;
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
            consumerConnectionsManager.add(channelName, this);
            packetHandler = new ConsumerPacketHandler(id, channelMessageQueues.getQueueCreateIfNotExists(channelName),
                                                      packetTimeoutManager, workerToWorkerConnectionsManager, this);
        } else if (packet.getClientType() == ClientType.PRODUCER_MAIN) {
            producerConnectionsManager.add(channelName, this);
            packetHandler = new MainProducerPacketHandler(id, channelName, packet.getBackups(), channelMessageQueues,
                                                          packetTimeoutManager, this);
        } else if (packet.getClientType() == ClientType.PRODUCER_BACKUP) {
            producerConnectionsManager.add(channelName, this);
            packetHandler = new BackupProducerPacketHandler(id, channelName, packet.getBackups(),
                                                            packet.getToleratedTimeout(), channelMessageQueues,
                                                            packetTimeoutManager, this);
        }
    }

    private void removeFromWorkerToClientConnections() {
        producerConnectionsManager.remove(channelName, this);
        consumerConnectionsManager.remove(channelName, this);
    }
}
