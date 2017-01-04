package com.hermes;

import com.hermes.client.ClientType;
import com.hermes.client.workerallocation.Worker;
import com.hermes.network.SocketClient;
import com.hermes.network.packet.AckPacket;
import com.hermes.network.packet.Packet;
import com.hermes.network.timeout.PacketTimeoutManager;
import com.hermes.network.timeout.TimeoutConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ProducerClient extends SocketClient {
    private ClientType clientType;
    private Worker serverWorker;
    private List<Worker> workerBackups;
    private PacketTimeoutManager packetTimeoutManager;
    private Thread receiverThread;
    private CompletableFuture<Void> callback;


    public ProducerClient(ClientType clientType, Worker serverWorker, List<Worker> workerBackups,
                          CompletableFuture<Void> callback) {
        super(serverWorker.getUrl());
        this.clientType = clientType;
        this.serverWorker = serverWorker;
        this.workerBackups = workerBackups;
        this.callback = callback;
        this.packetTimeoutManager = new PacketTimeoutManager();
        this.receiverThread = buildReceiverThread();
    }

    public ProducerClient(Worker serverWorker, CompletableFuture<Void> callback) {
        this(ClientType.PRODUCER_MAIN, serverWorker, Collections.emptyList(), callback);
    }

    @Override
    protected void run() {
        receiverThread.start();
    }

    private Thread buildReceiverThread() {
        return new Thread(() -> {
            try {
                Packet packet = readReply();
                while (packet != null) {
                    switch (packet.TYPE) {
                        case ACK:
                            packetTimeoutManager.messageReceived(((AckPacket)packet).ackMessageId);
                            break;
                        default:
                            System.out.println("Error - received unrecognized packet type " + packet.TYPE);
                    }
                    packet = readReply();
                }
            } catch (IOException e) {
                callback.completeExceptionally(e);
            }
        });
    }

    public void send(Packet packet, CompletableFuture<Void> sendPacketFuture) {
        packetTimeoutManager.add(packet.MESSAGE_ID, TimeoutConfig.TIMEOUT, sendPacketFuture);
        try {
            send(packet);
        } catch (Exception e) {
            sendPacketFuture.completeExceptionally(e);
        }
    }

    public ClientType getClientType() {
        return clientType;
    }

    public Worker getServerWorker() {
        return serverWorker;
    }

    public List<Worker> getWorkerBackups() {
        return workerBackups;
    }

    public void stop() {
        packetTimeoutManager.stop();
        receiverThread.stop();
        shutdown();
    }
}
