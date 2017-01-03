package com.hermes.network;

import com.hermes.network.packet.Packet;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public abstract class SocketServer {

    protected Set<SocketServerHandlerThread> threads;
    private volatile boolean shouldAcceptConnections = true;
    private int port;
    private ServerSocket socket;

    public SocketServer(int port) {
        this.port = port;
        this.threads = new HashSet<>();
    }

    public void start() {
        try {
            socket = new ServerSocket(port);
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        while(shouldAcceptConnections) {
            try {
                SocketServerHandlerThread thread = buildHandlerThread(socket.accept());
                threads.add(thread);
                thread.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void stop() {
        shouldAcceptConnections = false;
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Iterator<SocketServerHandlerThread> iterator = threads.iterator();
        while (iterator.hasNext()) {
            iterator.next().shutdown();
        }
        threads.clear();
    }

    protected void send(SocketServerHandlerThread thread, Packet packet) {
        try {
            thread.send(packet);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract SocketServerHandlerThread buildHandlerThread(Socket socket);
}
