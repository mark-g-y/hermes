package com.hermes;

import com.hermes.network.packet.ConsumerInitPacket;
import com.hermes.worker.metadata.Worker;
import com.hermes.worker.WorkerManager;
import com.hermes.zookeeper.ZKManager;
import org.apache.zookeeper.Watcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Consumer {
    private String channelName;
    private String groupName;
    private Receiver receiver;
    private List<ConsumerClient> clients;
    private AtomicBoolean isStopped;

    /**
     * Creates a new consumer to receive messages from Hermes.
     * @param zkUrl         The ZooKeeper URL (or comma separated URLs) of the ZooKeeper node(s).
     * @param channelName   The name of the channel to receive messages from.
     * @param groupName     The consumer group this consumer belongs to. A message is broadcast to each consumer group,
     *                      but only one consumer within each group will receive it (load balanced).
     * @param receiver      The interface containing logic to handle received messages.
     */
    public Consumer(String zkUrl, String channelName, String groupName, Receiver receiver) {
        ZKManager.init(zkUrl);
        this.channelName = channelName;
        this.groupName = groupName;
        this.receiver = receiver;
        this.clients = new ArrayList<>();
        this.isStopped = new AtomicBoolean(false);
    }

    /**
     * Starts the consumer. This method must be called before messages can be received.
     */
    public void start() {
        updateWorkers();
    }

    private synchronized void updateWorkers() {
        List<Worker> workers;
        try {
            workers = WorkerManager.getAllWorkersForChannel(channelName, (event) -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged && !isStopped.get()) {
                    // <TODO> replace isStopped check with removeWatches in ZooKeeper 3.5 when it is out of alpha and
                    // <TODO> fully released
                    updateWorkers();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        HashSet<String> oldWorkerIds = new HashSet<>(clients.stream()
                                                             .map((client) -> client.getServerWorker().getId())
                                                             .collect(Collectors.toSet()));
        HashSet<String> workerIds = new HashSet<>(workers.stream()
                                                             .map((worker) -> worker.getId())
                                                             .collect(Collectors.toSet()));
        for (int i = 0; i < clients.size(); i++) {
            if (!clients.get(i).isConnected() && workerIds.contains(clients.get(i).getServerWorker().getId())) {
                startClientForWorker(clients.get(i).getServerWorker());
            } else if (!workerIds.contains(clients.get(i).getServerWorker().getId())) {
                ConsumerClient oldClient = clients.remove(i);
                if (oldClient != null) {
                    oldClient.stop();
                }
            }
        }
        HashSet<String> newlyAddedWorkerIds = new HashSet<>(workerIds);
        newlyAddedWorkerIds.removeAll(oldWorkerIds);
        for (Worker worker: workers) {
            if (newlyAddedWorkerIds.contains(worker.getId())) {
                startClientForWorker(worker);
            }
        }
    }

    private void startClientForWorker(Worker worker) {
        ConsumerClient consumerClient = new ConsumerClient(worker, receiver);
        clients.add(consumerClient);
        consumerClient.start();
        try {
            consumerClient.init(new ConsumerInitPacket(channelName, groupName));
        } catch (Exception e) {
            // let backups handle failure via sending through different channel
        }
    }

    /**
     * Shuts down this consumer.
     */
    public void stop() {
        isStopped.set(true);
        for (ConsumerClient client : clients) {
            client.stop();
        }
    }
}
