package com.hermes;

import com.hermes.network.packet.InitPacket;
import com.hermes.network.packet.MessagePacket;
import com.hermes.test.UsesZooKeeperTest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class TestWorkerHandleMessages extends UsesZooKeeperTest {
    private static final String CHANNEL_BASE = "foobar";
    private static final long TIMEOUT = 3000;

    private Worker[] workers;
    private ProducerClient[] sendClients;
    private ConsumerClient[] receiveClients;

    @Test(dataProvider = "clientServerCombos")
    public void testMultipleClientsServersReceiveAndDeliverMessage(int numProducers, int numWorkers, int numConsumers)
            throws Exception {
        workers = new Worker[numWorkers];
        com.hermes.client.workerallocation.Worker[] workerData = new com.hermes.client.workerallocation.Worker[numWorkers];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(Integer.toString(i), "localhost", 3000 + i);
            workerData[i] = new com.hermes.client.workerallocation.Worker(Integer.toString(i), "localhost:" + (3000 + i));
        }
        startWorkers();

        sendClients = new ProducerClient[numProducers];
        CompletableFuture<Void>[] sendClientFutures = new CompletableFuture[sendClients.length];
        for (int i = 0; i < sendClients.length; i++) {
            sendClientFutures[i] = new CompletableFuture<>();
            sendClients[i] = new ProducerClient(workerData[i % workerData.length], sendClientFutures[i]);
            sendClients[i].start();
            sendClients[i].send(new InitPacket(InitPacket.ClientType.PRODUCER, CHANNEL_BASE + i, Collections.emptyList()),
                                sendClientFutures[i]);
        }

        receiveClients = new ConsumerClient[numConsumers];
        CompletableFuture<String>[] receiveMessageFutures = new CompletableFuture[receiveClients.length];
        for (int i = 0; i < receiveClients.length; i++) {
            int index = i;
            receiveMessageFutures[i] = new CompletableFuture<>();
            Receiver receiver = new Receiver() {
                @Override
                public void onMessageReceived(String message) {
                    receiveMessageFutures[index].complete(message);
                }
                @Override
                public void onDisconnect(Throwable throwable) {
                    receiveMessageFutures[index].completeExceptionally(throwable);
                }
            };
            receiveClients[i] = new ConsumerClient(workerData[i % workerData.length], receiver);
            receiveClients[i].start();
            receiveClients[i].init(new InitPacket(InitPacket.ClientType.CONSUMER, CHANNEL_BASE + i, Collections.emptyList()));
        }

        String baseMessage = "testing";
        CompletableFuture<Void>[] sendMessageFutures = new CompletableFuture[sendClients.length];
        for (int i = 0; i < sendClients.length; i++) {
            sendMessageFutures[i] = new CompletableFuture<>();
            String message = baseMessage + i;
            sendClients[i].send(new MessagePacket(message), sendMessageFutures[i]);
        }

        try {
            CompletableFuture.allOf(sendMessageFutures).get(TIMEOUT, TimeUnit.MILLISECONDS);
            for (int i = 0; i < receiveClients.length; i++) {
                String receivedMessage = receiveMessageFutures[i].get(TIMEOUT, TimeUnit.MILLISECONDS);
                Assert.assertEquals(baseMessage + i, receivedMessage);
            }
        } catch (Exception e) {
            Assert.fail("Fail!", e);
        }
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

    @DataProvider(name = "clientServerCombos")
    public Object[][] clientServerCombosProvider() {
        return new Object[][] {
                { 1, 1, 1 },
                { 3, 2, 2 },
                { 2, 1, 2 }
        };
    }

    private void startWorkers() throws Exception {
        for (Worker worker : workers) {
            new Thread(() -> worker.start()).start();
        }
        Thread.sleep(1000);
    }
}
