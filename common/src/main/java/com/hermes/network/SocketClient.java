package com.hermes.network;

import com.hermes.network.packet.Packet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public abstract class SocketClient {

    private String host;
    private int port;

    private Socket socket;
    private ObjectOutputStream oos;
    private ObjectInputStream ois;

    public SocketClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public SocketClient(String url) {
        this.host = url.split(":")[0];
        this.port = Integer.parseInt(url.split(":")[1]);
    }

    public void start() {
        try {
            socket = new Socket(host, port);
            oos = new ObjectOutputStream(socket.getOutputStream());
            ois = new ObjectInputStream(socket.getInputStream());
        } catch(Exception e){
            e.printStackTrace();
            return;
        }
        run();
    }

    public void shutdown() {
        try {
            oos.close();
            ois.close();
            socket.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    protected synchronized void send(Packet packet) throws IOException {
        oos.writeObject(packet);
    }

    protected Packet readReply() throws IOException {
        try {
            return (Packet) ois.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected abstract void run();
}
