package com.hermes.network;

import com.hermes.network.packet.Packet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public abstract class SocketServer {

    protected Set<HandlerThread> threads;
    private volatile boolean shouldAcceptConnections = true;
    private int port;
    private ServerSocket socket;

    public SocketServer(int port) {
        this.port = port;
        threads = new HashSet<>();
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
                HandlerThread thread = new HandlerThread(socket.accept());
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
        Iterator<HandlerThread> iterator = threads.iterator();
        while (iterator.hasNext()) {
            iterator.next().stopConnections();
        }
        threads.clear();
    }

    public class HandlerThread extends Thread {
        private Socket socket;
        ObjectInputStream ois;
        ObjectOutputStream oos;
        public HandlerThread(Socket socket) {
            super();
            try {
                this.socket = socket;
                this.ois = new ObjectInputStream(socket.getInputStream());
                this.oos = new ObjectOutputStream(socket.getOutputStream());
                onClientConnected(this);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        @Override
        public void run() {
            try {
                Packet packet;
                while((packet = (Packet)ois.readObject()) != null) {
                    onReceive(this, packet);
                }
                shutdown();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void stopConnections() {
            try {
                ois.close();
                oos.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void shutdown() {
            stopConnections();
            threads.remove(this);
        }
    }

    protected void send(HandlerThread thread, Packet packet) {
        try {
            thread.oos.writeObject(packet);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract void onClientConnected(HandlerThread thread);
    protected abstract void onReceive(HandlerThread thread, Packet packet);
}
