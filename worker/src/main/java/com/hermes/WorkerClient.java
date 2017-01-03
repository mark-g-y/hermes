package com.hermes;

import com.hermes.network.SocketClient;
import com.hermes.network.packet.Packet;

import java.io.IOException;

public class WorkerClient extends SocketClient {
    public WorkerClient(String url) {
        super(url);
    }

    @Override
    protected void run() {
    }

    @Override
    public void send(Packet packet) throws IOException {
        super.send(packet);
    }
}
