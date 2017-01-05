package com.hermes.server;

import com.hermes.WorkerClient;
import com.hermes.client.ClientType;
import com.hermes.worker.metadata.Worker;
import com.hermes.connection.ChannelClientConnectionsManager;
import com.hermes.connection.WorkerToWorkerConnectionsManager;
import com.hermes.message.ChannelMessageQueues;
import com.hermes.message.Message;
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
    private ClientType clientType;
    private List<Worker> backups;
    private long ackTimeout;
    private ChannelMessageQueues channelMessageQueues;
    private LinkedBlockingQueue<Message> messageQueue;
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
                handleAck((AckPacket)packet);
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
        messageQueue = channelMessageQueues.getQueueCreateIfNotExists(channelName);

        backups = packet.getBackups();
        ackTimeout = packet.getToleratedTimeout();

        clientType = packet.getClientType();
        if (clientType == ClientType.PRODUCER_MAIN || clientType == ClientType.PRODUCER_BACKUP) {
            producerConnectionsManager.add(packet.getChannelName(), this);
        } else if (clientType == ClientType.CONSUMER) {
            consumerConnectionsManager.add(packet.getChannelName(), this);
            createSendMessagesThread();
            sendMessagesThread.start();
        }
    }

    private void handleMessage(MessagePacket packet) {
        if (clientType == ClientType.PRODUCER_MAIN) {
            channelMessageQueues.add(channelName, new Message(packet, backups));
        } else if (clientType == ClientType.PRODUCER_BACKUP) {
            CompletableFuture<Void> ackFuture = new CompletableFuture<>();
            ackFuture.exceptionally((throwable) -> {
                channelMessageQueues.add(channelName, new Message(packet, backups));
                return null;
            });
            packetTimeoutManager.add(packet.MESSAGE_ID, ackTimeout, ackFuture);
        }
        sendAck(packet.MESSAGE_ID);
    }

    private void handleAck(AckPacket packet) {
        packetTimeoutManager.messageReceived(packet.ackMessageId);
    }

    private void sendAck(String messageId) {
        try {
            send(new AckPacket(messageId));
        } catch (IOException e) {
            // failures will be handled by backups
            e.printStackTrace();
        }
    }

    private void createSendMessagesThread() {
        this.sendMessagesThread = new Thread(() -> {
            try {
                while (true) {
                    Message message = messageQueue.take();
                    MessagePacket packet = message.getMessagePacket();
                    List<Worker> backups = message.getBackups();
                    CompletableFuture<Void> messageAckFuture = new CompletableFuture<>();
                    messageAckFuture.thenApply((v) -> {
                        for (Worker worker : backups) {
                            WorkerClient client = workerToWorkerConnectionsManager.getConnectionCreateIfNotExists(worker);
                            try {
                                client.send(new AckPacket(packet.MESSAGE_ID));
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
