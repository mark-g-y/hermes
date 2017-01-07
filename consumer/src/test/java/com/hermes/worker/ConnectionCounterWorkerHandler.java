package com.hermes.worker;

import com.hermes.network.packet.Packet;

import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionCounterWorkerHandler extends AbstractMockWorkerHandler {
    private AtomicInteger numPreviousAndCurrentConnections;

    public ConnectionCounterWorkerHandler(String id, int port, Socket socket,
                                          AtomicInteger numPreviousAndCurrentConnections) {
        super(id, port, socket);
        this.numPreviousAndCurrentConnections = numPreviousAndCurrentConnections;
    }

    @Override
    protected void onClientConnected() {
        numPreviousAndCurrentConnections.getAndIncrement();
    }

    @Override
    protected void onReceive(Packet packet) {
    }

    @Override
    protected void onDisconnect() {
    }
}
