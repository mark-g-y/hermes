package com.hermes.server;

import com.hermes.connection.ConsumerConnectionsManager;
import com.hermes.connection.ProducerConnectionsManager;
import com.hermes.connection.WorkerToWorkerConnectionsManager;
import com.hermes.loadbalance.LoadRebalancer;
import com.hermes.loadbalance.MonitorLoadThread;
import com.hermes.message.MessageQueues;
import com.hermes.network.SocketServer;
import com.hermes.network.SocketServerHandler;
import com.hermes.network.timeout.PacketTimeoutManager;

import java.net.Socket;

public class WorkerSocketServer extends SocketServer {
    private String id;
    private MessageQueues messageQueues;
    private PacketTimeoutManager packetTimeoutManager;
    private ProducerConnectionsManager producerConnectionsManager;
    private ConsumerConnectionsManager consumerConnectionsManager;
    private WorkerToWorkerConnectionsManager workerToWorkerConnectionsManager;

    private MonitorLoadThread monitorLoadThread;

    public WorkerSocketServer(String id, int port) {
        super(port);
        this.id = id;
        this.messageQueues = new MessageQueues();
        this.packetTimeoutManager = new PacketTimeoutManager();
        this.producerConnectionsManager = new ProducerConnectionsManager();
        this.consumerConnectionsManager = new ConsumerConnectionsManager();
        this.workerToWorkerConnectionsManager = new WorkerToWorkerConnectionsManager();

        this.monitorLoadThread = new MonitorLoadThread(id, new LoadRebalancer(producerConnectionsManager));
    }

    @Override
    public void start() {
        monitorLoadThread.start();
        super.start();
    }

    @Override
    public void stop() {
        monitorLoadThread.interrupt();
        super.stop();
    }

    @Override
    protected SocketServerHandler buildHandler(Socket socket) {
        return new WorkerServerHandler(socket, id, messageQueues, packetTimeoutManager,
                                       producerConnectionsManager, consumerConnectionsManager,
                                       workerToWorkerConnectionsManager);
    }
}
