package com.hermes;

import com.hermes.client.ClientType;
import com.hermes.network.packet.InitPacket;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.timeout.TimeoutConfig;
import com.hermes.test.UsesZooKeeperTest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class TestWorkerBackups extends UsesZooKeeperTest {
    private static final String CHANNEL = "foobar";

    private Worker[] workers;
    private ProducerClient[] sendClients;
    private ConsumerClient[] receiveClients;

    @Test
    public void testWorkerFailure() throws Exception {
        workers = new Worker[3];
        com.hermes.worker.metadata.Worker[] workerData = new com.hermes.worker.metadata.Worker[workers.length];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(Integer.toString(i), "localhost", 3000 + i);
            workerData[i] = new com.hermes.worker.metadata.Worker(Integer.toString(i), "localhost:" + (3000 + i), 0);
        }
        startWorkers();

        String sentMessage = "testing";
        AtomicInteger numMessageReceptions = new AtomicInteger(0);

        sendClients = new ProducerClient[3];
        CompletableFuture<Void> sendCallback = new CompletableFuture<>(); // dummy variable - won't do anything
        List<com.hermes.worker.metadata.Worker> backups = new ArrayList(Arrays.asList(workerData));
        backups.remove(0);
        sendClients[0] = new ProducerClient(ClientType.PRODUCER_MAIN, workerData[0], backups, sendCallback);
        sendClients[0].start();
        sendClients[0].send(new InitPacket(ClientType.PRODUCER_MAIN, CHANNEL, backups, TimeoutConfig.TIMEOUT),
                                           sendCallback);
        for (int i = 1; i < sendClients.length; i++) {
            backups = new ArrayList<>(backups);
            backups.remove(0);
            sendClients[i] = new ProducerClient(ClientType.PRODUCER_BACKUP, workerData[i], backups, sendCallback);
            sendClients[i].start();
            sendClients[i].send(new InitPacket(ClientType.PRODUCER_BACKUP, CHANNEL, backups,
                                               (i + 1) * TimeoutConfig.TIMEOUT), sendCallback);
        }

        receiveClients = new ConsumerClient[3];
        for (int i = 0; i < receiveClients.length; i++) {
            receiveClients[i] = new ConsumerClient(workerData[i], new Receiver() {
                @Override
                public void onMessageReceived(String message) {
                    if (sentMessage.equals(message)) {
                        numMessageReceptions.getAndIncrement();
                    }
                }
                @Override
                public void onDisconnect(Throwable throwable) {
                }
            });
            receiveClients[i].start();
            receiveClients[i].init(new InitPacket(ClientType.CONSUMER, CHANNEL));
        }

        workers[0].stop();

        MessagePacket messagePacket = new MessagePacket(sentMessage);
        for (int i = 0; i < sendClients.length; i++) {
            sendClients[i].send(messagePacket, sendCallback);
        }

        Thread.sleep((long)(TimeoutConfig.TIMEOUT * 3.5));

        Assert.assertEquals(1, numMessageReceptions.get());
    }

    @AfterMethod
    public void afterMethod() {
        for (Worker worker : workers) {
            worker.stop();
        }
        for (ProducerClient sendClient : sendClients) {
            sendClient.stop();
        }
        for (ConsumerClient receiveClient : receiveClients) {
            receiveClient.stop();
        }
    }

    private void startWorkers() throws Exception {
        for (Worker worker : workers) {
            new Thread(() -> worker.start()).start();
        }
        Thread.sleep(1000);
    }
}
