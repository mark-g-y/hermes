package com.hermes;

import com.hermes.client.workerallocation.Worker;
import com.hermes.client.workerallocation.WorkerManager;
import com.hermes.network.PacketSendClient;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.packet.Packet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Producer {
    private static final int NUM_SEND_TRIES = 3;
    private String channelName;
    private List<PacketSendClient> clients;
    private ReentrantReadWriteLock workerLock;

    public Producer(String channelName) {
        this.channelName = channelName;
        this.clients = new ArrayList<>();
        this.workerLock = new ReentrantReadWriteLock();
    }

    public void start() {
        workerLock.writeLock().lock();
        List<Worker> workers;
        try {
            // <TODO> add backups (i.e. select more than 1 worker, with extras being backup)
            workers = WorkerManager.selectWorkers(WorkerManager.getAllWorkersForChannel(channelName), 1);
        } catch (Exception e) {
            workerLock.writeLock().unlock();
            e.printStackTrace();
            return;
        }
        for (Worker worker : workers) {
            startClientForWorker(worker);
        }
        // <TODO> when multiple workers serving, need to send config messages to each here
        workerLock.writeLock().unlock();
    }

    private void startClientForWorker(Worker worker) {
        CompletableFuture<Void> packetSendClientCallback = new CompletableFuture<>();
        PacketSendClient client = new PacketSendClient(worker, packetSendClientCallback);
        packetSendClientCallback.exceptionally((throwable) -> {
            handleConnectionLost(throwable, client);
            return null;
        });
        client.start();
        clients.add(client);
    }

    private void handleConnectionLost(Throwable th, PacketSendClient client) {
        try {
            workerLock.writeLock().lock();
            clients.remove(client);
            Worker newWorker = getNewWorker();
            startClientForWorker(newWorker);
            // <TODO> when multiple workers serving, need to send config messages to each here
            workerLock.writeLock().unlock();
        } catch (Exception e) {
            workerLock.writeLock().unlock();
            e.printStackTrace();
        }
    }

    private Worker getNewWorker() throws Exception {
        Set<Worker> existingWorkers = clients.stream().map((client) -> client.getServerWorker()).collect(Collectors.toSet());
        return WorkerManager.selectWorkers(WorkerManager.getAllWorkersForChannel(channelName), 1, existingWorkers).get(0);
    }

    public void send(String message, ProducerCallback callback) {
        MessagePacket packet = new MessagePacket(message);
        for (int i = 0; i < NUM_SEND_TRIES; i++) {
            final int tryNum = i;
            CompletableFuture sendPacketFuture = CompletableFuture.anyOf(sendPacketToAllClients(packet));
            sendPacketFuture.thenApply((v) -> {
                callback.onSuccess();
                return null;
            });
            sendPacketFuture.exceptionally((throwable) -> {
                if (tryNum >= NUM_SEND_TRIES) {
                    callback.onFailure((Throwable)throwable);
                } else {
                    sendPacketToAllClients(packet);
                }
                return null;
            });
        }
    }

    private CompletableFuture[] sendPacketToAllClients(Packet packet) {
        workerLock.readLock().lock();
        CompletableFuture<Void>[] futures = new CompletableFuture[clients.size()];
        for (int m = 0; m < clients.size(); m++) {
            CompletableFuture<Void> sendPacketFuture = new CompletableFuture<>();
            futures[m] = sendPacketFuture;
            clients.get(m).send(packet, sendPacketFuture);
        }
        workerLock.readLock().unlock();
        return futures;
    }

    public void stop() {
        workerLock.readLock().lock();
        for (PacketSendClient client : clients) {
            client.stop();
        }
        workerLock.readLock().unlock();
    }
}
