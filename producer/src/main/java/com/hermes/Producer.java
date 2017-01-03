package com.hermes;

import com.hermes.client.workerallocation.Worker;
import com.hermes.client.workerallocation.WorkerManager;
import com.hermes.network.packet.InitPacket;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.packet.Packet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Producer {
    private static final int NUM_SEND_TRIES = 3;
    private String channelName;
    private List<ProducerClient> clients;
    private ReentrantReadWriteLock workerLock;
    private AtomicBoolean isStopped;

    public Producer(String channelName) {
        this.channelName = channelName;
        this.clients = new ArrayList<>();
        this.workerLock = new ReentrantReadWriteLock();
        this.isStopped = new AtomicBoolean(false);
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
        workerLock.writeLock().unlock();
    }

    private void startClientForWorker(Worker worker) {
        CompletableFuture<Void> packetSendClientCallback = new CompletableFuture<>();
        ProducerClient client = new ProducerClient(worker, packetSendClientCallback);
        packetSendClientCallback.exceptionally((throwable) -> {
            handleConnectionLost(throwable, client);
            return null;
        });
        clients.add(client);
        client.start();
        // <TODO> backups should not be an empty list once feature is implemented
        client.send(new InitPacket(InitPacket.ClientType.PRODUCER, channelName, Collections.emptyList()),
                    packetSendClientCallback);
    }

    private void handleConnectionLost(Throwable th, ProducerClient client) {
        if (isStopped.get()) {
            return;
        }
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
        isStopped.set(true);
        workerLock.readLock().lock();
        for (ProducerClient client : clients) {
            client.stop();
        }
        workerLock.readLock().unlock();
    }
}
