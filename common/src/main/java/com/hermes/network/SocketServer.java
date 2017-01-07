package com.hermes.network;

import com.hermes.network.packet.Packet;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SocketServer {

    protected Set<SocketServerHandler> serverHandlers;
    private AtomicBoolean shouldAcceptConnections;
    private int port;
    private ServerSocket socket;

    public SocketServer(int port) {
        this.port = port;
        this.shouldAcceptConnections = new AtomicBoolean(true);
        this.serverHandlers = new HashSet<>();
    }

    public void start() {
        try {
            socket = new ServerSocket(port);
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        while(shouldAcceptConnections.get()) {
            try {
                SocketServerHandler handler = buildHandler(socket.accept());
                serverHandlers.add(handler);
                new Thread(handler).start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void stop() {
        shouldAcceptConnections.set(false);
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Iterator<SocketServerHandler> iterator = serverHandlers.iterator();
        while (iterator.hasNext()) {
            iterator.next().shutdown();
        }
        serverHandlers.clear();
    }

    protected void send(SocketServerHandler thread, Packet packet) {
        try {
            thread.send(packet);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract SocketServerHandler buildHandler(Socket socket);
}
