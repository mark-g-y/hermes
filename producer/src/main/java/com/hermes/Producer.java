package com.hermes;

import com.hermes.client.ClientType;
import com.hermes.network.packet.ProducerInitPacket;
import com.hermes.partition.Partition;
import com.hermes.worker.metadata.Worker;
import com.hermes.worker.WorkerManager;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.packet.Packet;
import com.hermes.network.timeout.TimeoutConfig;
import com.hermes.zookeeper.ZKManager;

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

    /**
     * Creates a new producer object that can be used to publish messages through Hermes.
     * @param zkUrl             The ZooKeeper URL (or comma separated URLs) of the ZooKeeper node(s).
     * @param channelName       Name of the channel you want to messages to.
     * @param numWorkerBackups  The number of backups (excludes master) used to ensure fault tolerance.
     */
    public Producer(String zkUrl, String channelName, int numWorkerBackups) {
        ZKManager.init(zkUrl);
        this.channelName = channelName;
        this.numWorkerBackups = numWorkerBackups;
        this.clients = new ArrayList<>();
        this.workerLock = new ReentrantReadWriteLock();
        this.isStopped = new AtomicBoolean(false);
    }

    /**
     * Creates a new producer object that can be used to publish messages through Hermes. This producer does not use
     * backups to ensure fault tolerance.
     * @param zkUrl         The ZooKeeper URL (or comma separated URLs) of the ZooKeeper node(s).
     * @param channelName   Name of the channel you want to messages to.
     */
    public Producer(String zkUrl, String channelName) {
        this(zkUrl, channelName, 0);
    }

    /**
     * Starts the producer. This method must be called before messages can be sent.
     */
    public void start() {
        workerLock.writeLock().lock();
        List<Worker> workers;
        try {
            workers = WorkerManager.selectWorkersForPartition(Partition.get(channelName), 1 + numWorkerBackups);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
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
        sendInitPackets();
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
    }

    private void handleConnectionLost(Throwable th, ProducerClient client) {
        if (isStopped.get()) {
            return;
        }
        try {
            workerLock.writeLock().lock();
            clients.remove(client);
            Worker newWorker = getNewWorker();
            updateWorkerBackups(client.getServerWorker(), newWorker);
            startClientForWorker(client.getClientType(), newWorker, client.getWorkerBackups());
            sendInitPackets();
            workerLock.writeLock().unlock();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void updateWorkerBackups(Worker oldBackup, Worker newBackup) {
        for (ProducerClient client : clients) {
            client.replaceWorkerBackup(oldBackup, newBackup);
        }
    }

    private void sendInitPackets() {
        for (ProducerClient client : clients) {
            client.send(new ProducerInitPacket(client.getClientType(), channelName, client.getWorkerBackups(),
                                               TimeoutConfig.TIMEOUT * (clients.size() - client.getWorkerBackups().size())),
                        new CompletableFuture<>());
        }
    }

    private Worker getNewWorker() throws Exception {
        Set<Worker> existingWorkers = clients.stream().map((client) -> client.getServerWorker()).collect(Collectors.toSet());
        return WorkerManager.selectWorkersForPartition(Partition.get(channelName), 1, existingWorkers).get(0);
    }

    /**
     * Sends a message.
     * @param message   The message to be sent.
     * @param callback  Callback for successful/unsuccessful delivery of message
     */
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

    /**
     * Shuts down this producer.
     */
    public void stop() {
        isStopped.set(true);
        workerLock.readLock().lock();
        for (ProducerClient client : clients) {
            client.stop();
        }
        workerLock.readLock().unlock();
    }
}
