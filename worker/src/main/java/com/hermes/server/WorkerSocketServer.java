package com.hermes.server;

import com.hermes.connection.ChannelClientConnectionsManager;
import com.hermes.connection.WorkerToWorkerConnectionsManager;
import com.hermes.message.ChannelMessageQueues;
import com.hermes.network.SocketServer;
import com.hermes.network.SocketServerHandlerThread;
import com.hermes.network.timeout.PacketTimeoutManager;

import java.net.Socket;

public class WorkerSocketServer extends SocketServer {
    private String id;
    private ChannelMessageQueues channelMessageQueues;
    private PacketTimeoutManager packetTimeoutManager;
    private ChannelClientConnectionsManager producerConnectionsManager;
    private ChannelClientConnectionsManager consumerConnectionsManager;
    private WorkerToWorkerConnectionsManager workerToWorkerConnectionsManager;

    public WorkerSocketServer(String id, int port) {
        super(port);
        this.id = id;
        this.channelMessageQueues = new ChannelMessageQueues();
        this.packetTimeoutManager = new PacketTimeoutManager();
        this.producerConnectionsManager = new ChannelClientConnectionsManager();
        this.consumerConnectionsManager = new ChannelClientConnectionsManager();
        this.workerToWorkerConnectionsManager = new WorkerToWorkerConnectionsManager();
    }

    @Override
    protected SocketServerHandlerThread buildHandlerThread(Socket socket) {
        return new WorkerServerHandlerThread(socket, id, channelMessageQueues, packetTimeoutManager,
                                             producerConnectionsManager, consumerConnectionsManager,
                                             workerToWorkerConnectionsManager);
    }
}
