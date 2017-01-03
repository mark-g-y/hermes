package com.hermes.worker;

import com.hermes.network.SocketServer;
import com.hermes.network.SocketServerHandlerThread;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.timeout.PacketTimeoutManager;
import com.hermes.network.timeout.TimeoutConfig;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MessageBroadcastSocketServer extends SocketServer {
    private String id;
    private int port;
    private PacketTimeoutManager packetTimeoutManager;

    public MessageBroadcastSocketServer(String id, int port) {
        super(port);
        this.id = id;
        this.port = port;
        this.packetTimeoutManager = new PacketTimeoutManager();
    }

    @Override
    public void start() {
        try {
            ZKUtility.createIgnoreExists(ZKManager.get(), ZKPaths.WORKERS + "/" + id, ("localhost:" + port).getBytes(),
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.start();
    }

    public String getId() {
        return id;
    }

    public List<CompletableFuture<Void>> broadcastMessage(String message) {
        List<CompletableFuture<Void>> messageAckFutures = new ArrayList<>();
        MessagePacket packet = new MessagePacket(message);
        CompletableFuture<Void> messageAckFuture = new CompletableFuture<>();
        messageAckFutures.add(messageAckFuture);
        packetTimeoutManager.add(packet.MESSAGE_ID, TimeoutConfig.TIMEOUT, messageAckFuture);
        for (SocketServerHandlerThread thread : threads) {
            try {
                send(thread, packet);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return messageAckFutures;
    }

    @Override
    protected SocketServerHandlerThread buildHandlerThread(Socket socket) {
        return new MessageBroadcastWorkerThread(id, port, socket, packetTimeoutManager);
    }
}
