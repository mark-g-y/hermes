package com.hermes.network;

import com.hermes.network.packet.Packet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public abstract class SocketServerHandlerThread extends Thread {
    private Socket socket;
    private ObjectInputStream ois;
    private ObjectOutputStream oos;

    public SocketServerHandlerThread(Socket socket) {
        super();
        try {
            this.socket = socket;
            this.ois = new ObjectInputStream(socket.getInputStream());
            this.oos = new ObjectOutputStream(socket.getOutputStream());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public final void run() {
        onClientConnected();
        try {
            Packet packet;
            while((packet = (Packet)ois.readObject()) != null) {
                onReceive(packet);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        shutdown();
    }

    public final void send(Packet packet) throws IOException {
        oos.writeObject(packet);
    }

    public final void stopConnections() {
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
        onDisconnect();
    }

    protected abstract void onClientConnected();
    protected abstract void onReceive(Packet packet);
    protected abstract void onDisconnect();
}
