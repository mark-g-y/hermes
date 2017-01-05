package com.hermes;

import com.hermes.client.ClientType;
import com.hermes.worker.metadata.Worker;
import com.hermes.worker.WorkerManager;
import com.hermes.network.packet.InitPacket;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.packet.Packet;
import com.hermes.network.timeout.TimeoutConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Producer {
    private static final int NUM_SEND_TRIES = 3;
    private String channelName;
    private int numWorkerBackups;
    private List<ProducerClient> clients;
    private ReentrantReadWriteLock workerLock;
    private AtomicBoolean isStopped;

    public Producer(String channelName, int numWorkerBackups) {
        this.channelName = channelName;
        this.numWorkerBackups = numWorkerBackups;
        this.clients = new ArrayList<>();
        this.workerLock = new ReentrantReadWriteLock();
        this.isStopped = new AtomicBoolean(false);
    }

    public Producer(String channelName) {
        this(channelName, 0);
    }

    public void start() {
        workerLock.writeLock().lock();
        List<Worker> workers;
        try {
            workers = WorkerManager.selectWorkers(WorkerManager.getAllWorkersForChannel(channelName), 1 + numWorkerBackups);
        } catch (Exception e) {
            workerLock.writeLock().unlock();
            e.printStackTrace();
            // <TODO> figure out better way to handle not enough workers error
            return;
        }
        List<Worker> workerBackups = new ArrayList<>(workers);
        workerBackups.remove(0);
        startClientForWorker(ClientType.PRODUCER_MAIN, workers.get(0), workerBackups);
        for (int i = 1; i < workers.size(); i++) {
            workerBackups = new ArrayList<>(workerBackups);
            workerBackups.remove(0);
            startClientForWorker(ClientType.PRODUCER_BACKUP, workers.get(i), workerBackups);
        }
        workerLock.writeLock().unlock();
    }

    private void startClientForWorker(ClientType clientType, Worker worker, List<Worker> workerBackups) {
        CompletableFuture<Void> packetSendClientCallback = new CompletableFuture<>();
        ProducerClient client = new ProducerClient(clientType, worker, workerBackups, packetSendClientCallback);
        packetSendClientCallback.exceptionally((throwable) -> {
            handleConnectionLost(throwable, client);
            return null;
        });
        clients.add(client);
        client.start();
        client.send(new InitPacket(clientType, channelName, workerBackups,
                                   TimeoutConfig.TIMEOUT * (clients.size() - workerBackups.size())),
                    new CompletableFuture<>());
    }

    private void handleConnectionLost(Throwable th, ProducerClient client) {
        if (isStopped.get()) {
            return;
        }
        try {
            workerLock.writeLock().lock();
            clients.remove(client);
            Worker newWorker = getNewWorker();
            startClientForWorker(client.getClientType(), newWorker, client.getWorkerBackups());
            workerLock.writeLock().unlock();
        } catch (Exception e) {
            workerLock.writeLock().unlock();
            e.printStackTrace();
            // <TODO> figure out better way to handle not enough workers error
        }
    }

    private Worker getNewWorker() throws Exception {
        Set<Worker> existingWorkers = clients.stream().map((client) -> client.getServerWorker()).collect(Collectors.toSet());
        return WorkerManager.selectWorkers(WorkerManager.getAllWorkersForChannel(channelName), 1, existingWorkers).get(0);
    }

    public void send(String message, ProducerCallback callback) {
        MessagePacket packet = new MessagePacket(message);
        send(packet, 0, callback);
    }

    private void send(Packet packet, int tryNum, ProducerCallback callback) {
        CompletableFuture sendPacketFuture = CompletableFuture.anyOf(sendPacketToAllClients(packet));
        sendPacketFuture.thenApply((v) -> {
            callback.onSuccess();
            return null;
        });
        sendPacketFuture.exceptionally((throwable) -> {
            if (tryNum >= NUM_SEND_TRIES) {
                callback.onFailure((Throwable)throwable);
            } else {
                send(packet, tryNum + 1, callback);
            }
            return null;
        });
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
