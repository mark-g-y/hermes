package com.hermes.server.packethandler;

import com.hermes.WorkerClient;
import com.hermes.connection.WorkerToWorkerConnectionsManager;
import com.hermes.message.Message;
import com.hermes.network.packet.AckPacket;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.timeout.PacketTimeoutManager;
import com.hermes.network.timeout.TimeoutConfig;
import com.hermes.server.ServerToClientSender;
import com.hermes.worker.metadata.Worker;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsumerPacketHandler extends PacketHandler {
    private LinkedBlockingQueue<Message> messageQueue;
    private WorkerToWorkerConnectionsManager workerToWorkerConnectionsManager;
    private Thread sendMessagesThread;

    public ConsumerPacketHandler(String workerId, LinkedBlockingQueue<Message> messageQueue,
                                 PacketTimeoutManager packetTimeoutManager,
                                 WorkerToWorkerConnectionsManager workerToWorkerConnectionsManager,
                                 ServerToClientSender sender) {
        super(workerId, packetTimeoutManager, sender);
        this.messageQueue = messageQueue;
        this.workerToWorkerConnectionsManager = workerToWorkerConnectionsManager;
        createSendMessagesThread();
        sendMessagesThread.start();
    }

    @Override
    public void onMessage(MessagePacket packet) {
        throw new RuntimeException("Consumers cannot send message packets");
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
                        sender.send(packet);
                    } catch (IOException e) {
                        // do nothing - backups will handle failure scenario
                    }
                }
            } catch (InterruptedException e) {
                // should stop if interrupted
            }
        });
    }

    @Override
    public void stop() {
        sendMessagesThread.interrupt();
    }
}
