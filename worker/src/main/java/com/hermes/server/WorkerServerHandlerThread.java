package com.hermes.server;

import com.hermes.WorkerClient;
import com.hermes.client.workerallocation.Worker;
import com.hermes.connection.ChannelClientConnectionsManager;
import com.hermes.connection.WorkerToWorkerConnectionsManager;
import com.hermes.message.ChannelMessageQueues;
import com.hermes.network.SocketServerHandlerThread;
import com.hermes.network.packet.*;
import com.hermes.network.timeout.PacketTimeoutManager;
import com.hermes.network.timeout.TimeoutConfig;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public class WorkerServerHandlerThread extends SocketServerHandlerThread {
    private String id;
    private String channelName;
    private List<Worker> backups;
    private ChannelMessageQueues channelMessageQueues;
    private LinkedBlockingQueue<MessagePacket> messageQueue;
    private PacketTimeoutManager packetTimeoutManager;
    private ChannelClientConnectionsManager producerConnectionsManager;
    private ChannelClientConnectionsManager consumerConnectionsManager;
    private WorkerToWorkerConnectionsManager workerToWorkerConnectionsManager;
    private Thread sendMessagesThread;

    public WorkerServerHandlerThread(Socket socket,
                                     String id,
                                     ChannelMessageQueues channelMessageQueues,
                                     PacketTimeoutManager packetTimeoutManager,
                                     ChannelClientConnectionsManager producerConnectionsManager,
                                     ChannelClientConnectionsManager consumerConnectionsManager,
                                     WorkerToWorkerConnectionsManager workerToWorkerConnectionsManager) {
        super(socket);
        this.id = id;
        this.channelMessageQueues = channelMessageQueues;
        this.packetTimeoutManager = packetTimeoutManager;
        this.producerConnectionsManager = producerConnectionsManager;
        this.consumerConnectionsManager = consumerConnectionsManager;
        this.workerToWorkerConnectionsManager = workerToWorkerConnectionsManager;
        createSendMessagesThread();
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
                assignPartition((AssignPartitionPacket)packet);
                break;
            case INIT:
                handleInit((InitPacket)packet);
                break;
            case MESSAGE:
                handleMessage((MessagePacket)packet);
                break;
            case ACK:
                packetTimeoutManager.messageReceived(packet.MESSAGE_ID);
                break;
            default:
                System.out.println("Error - received unrecognized packet type " + packet.TYPE);
        }
    }

    @Override
    protected void onDisconnect() {
        sendMessagesThread.interrupt();
        removeFromWorkerToClientConnections();
    }

    private void assignPartition(AssignPartitionPacket packet) {
        ZooKeeper zk = ZKManager.get();
        try {
            // <TODO> decide whether maintain list of channels this worker is responsible for? only use case is if
            // <TODO> same worker is reassigned to different partition. The ephermeral won't die because worker is still
            // <TODO> alive, but worker should be removed from partition
            ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + packet.getPartition() + "/" + id, null,
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            send(new AckPacket(packet.MESSAGE_ID));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleInit(InitPacket packet) {
        sendMessagesThread.interrupt();
        removeFromWorkerToClientConnections();

        channelName = packet.getChannelName();
        messageQueue = channelMessageQueues.getMessageQueue(channelName);
        backups = packet.getBackups();
        if (packet.getClientType() == InitPacket.ClientType.PRODUCER) {
            producerConnectionsManager.add(packet.getChannelName(), this);
        } else if (packet.getClientType() == InitPacket.ClientType.CONSUMER) {
            consumerConnectionsManager.add(packet.getChannelName(), this);
            createSendMessagesThread();
            sendMessagesThread.start();
        }
    }

    private void handleMessage(MessagePacket packet) {
        channelMessageQueues.add(channelName, packet);
        try {
            send(new AckPacket(packet.MESSAGE_ID));
        } catch (IOException e) {
            // failures will be handled by backups
            e.printStackTrace();
        }
    }

    private void createSendMessagesThread() {
        this.sendMessagesThread = new Thread(() -> {
            try {
                while (true) {
                    MessagePacket packet;
                    packet = messageQueue.take();
                    CompletableFuture<Void> messageAckFuture = new CompletableFuture<>();
                    messageAckFuture.thenApply((v) -> {
                        for (Worker worker : backups) {
                            WorkerClient client = workerToWorkerConnectionsManager.getConnectionCreateIfNotExists(worker);
                            try {
                                client.send(packet);
                            } catch (IOException e) {
                                workerToWorkerConnectionsManager.removeConnection(worker);
                            }
                        }
                        return null;
                    });
                    packetTimeoutManager.add(packet.MESSAGE_ID, TimeoutConfig.TIMEOUT, messageAckFuture);
                    try {
                        send(packet);
                    } catch (IOException e) {
                        // do nothing - backups will handle failure scenario
                    }
                }
            } catch (InterruptedException e) {
                // should stop if interrupted
            }
        });
    }

    private void removeFromWorkerToClientConnections() {
        producerConnectionsManager.remove(channelName, this);
        consumerConnectionsManager.remove(channelName, this);
    }
}
