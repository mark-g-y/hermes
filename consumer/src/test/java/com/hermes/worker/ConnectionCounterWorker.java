package com.hermes.worker;

import com.hermes.network.packet.Packet;

import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionCounterWorker extends AbstractMockWorker {
    private AtomicInteger numPreviousAndCurrentConnections;

    public ConnectionCounterWorker(String id, int port) throws Exception {
        super(id, port);
        this.numPreviousAndCurrentConnections = new AtomicInteger(0);
    }

    @Override
    protected void onClientConnected(HandlerThread thread) {
        numPreviousAndCurrentConnections.getAndIncrement();
    }

    @Override
    protected void onReceive(HandlerThread thread, Packet packet) {
    }

    public int getNumPreviousAndCurrentConnections() {
        return numPreviousAndCurrentConnections.get();
    }
}
