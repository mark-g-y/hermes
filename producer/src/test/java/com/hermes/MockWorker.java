package com.hermes;

import com.hermes.network.SocketServer;
import com.hermes.network.SocketServerHandlerThread;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class MockWorker extends SocketServer {
    private List<String> receivedMessages;

    public MockWorker(int port) {
        super(port);
        this.receivedMessages = new ArrayList<>();
    }

    public List<String> getReceivedMessages() {
        return receivedMessages;
    }

    @Override
    protected SocketServerHandlerThread buildHandlerThread(Socket socket) {
        return new MockWorkerThread(socket, receivedMessages);
    }
}
