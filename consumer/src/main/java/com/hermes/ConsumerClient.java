package com.hermes;

import com.hermes.client.workerallocation.Worker;
import com.hermes.network.SocketClient;
import com.hermes.network.packet.AckPacket;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.packet.Packet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerClient extends SocketClient {
    private Worker worker;
    private Receiver receiver;
    private Thread receiverThread;
    private AtomicBoolean isConnected;

    public ConsumerClient(Worker worker, Receiver receiver) {
        super(worker.getUrl());
        this.worker = worker;
        this.receiver = receiver;
        this.isConnected = new AtomicBoolean();
    }

    @Override
    protected void run() {
        isConnected.set(true);
        receiverThread = new Thread(() -> {
            try {
                Packet packet = readReply();
                while (packet != null) {
                    switch (packet.TYPE) {
                        case MESSAGE:
                            receiver.onMessageReceived(((MessagePacket)packet).getMessage());
                            send(new AckPacket(packet.MESSAGE_ID));
                            break;
                        default:
                            System.out.println("Error - received unrecognized packet type " + packet.TYPE);
                    }
                    packet = readReply();
                }
            } catch (IOException e) {
                isConnected.set(false);
                receiver.onDisconnect(e);
            }
        });
        receiverThread.start();
    }

    public boolean isConnected() {
        return isConnected.get();
    }

    public Worker getServerWorker() {
        return worker;
    }

    public void stop() {
        super.shutdown();
    }
}
