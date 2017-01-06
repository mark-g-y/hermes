package com.hermes;

import com.hermes.client.ClientType;
import com.hermes.network.packet.ConsumerInitPacket;
import com.hermes.network.packet.MessagePacket;
import com.hermes.network.packet.ProducerInitPacket;
import com.hermes.test.UsesZooKeeperTest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
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
        com.hermes.worker.metadata.Worker[] workerData = new com.hermes.worker.metadata.Worker[numWorkers];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(Integer.toString(i), "localhost", 3000 + i);
            workerData[i] = new com.hermes.worker.metadata.Worker(Integer.toString(i), "localhost:" + (3000 + i), 0);
        }
        startWorkers();

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
            receiveClients[i].init(new ConsumerInitPacket(CHANNEL_BASE + i, Integer.toString(i)));
        }

        sendClients = new ProducerClient[numProducers];
        CompletableFuture<Void>[] sendClientFutures = new CompletableFuture[sendClients.length];
        for (int i = 0; i < sendClients.length; i++) {
            sendClientFutures[i] = new CompletableFuture<>();
            sendClients[i] = new ProducerClient(workerData[i % workerData.length], sendClientFutures[i]);
            sendClients[i].start();
            sendClients[i].send(new ProducerInitPacket(ClientType.PRODUCER_MAIN, CHANNEL_BASE + i),
                                sendClientFutures[i]);
        }

        Thread.sleep(1000);

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

    @Test(dataProvider = "consumerGroupsProvider")
    public void testConsumerGroupMessageDelivery(String[] consumerGroups)
            throws Exception {
        workers = new Worker[1];
        com.hermes.worker.metadata.Worker[] workerData = new com.hermes.worker.metadata.Worker[1];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(Integer.toString(i), "localhost", 3000 + i);
            workerData[i] = new com.hermes.worker.metadata.Worker(Integer.toString(i), "localhost:" + (3000 + i), 0);
        }
        startWorkers();

        receiveClients = new ConsumerClient[consumerGroups.length];
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
            receiveClients[i].init(new ConsumerInitPacket(CHANNEL_BASE, consumerGroups[i]));
        }

        CompletableFuture<Void> sendClientFuture = new CompletableFuture<>();
        sendClients = new ProducerClient[] {new ProducerClient(workerData[0], sendClientFuture)};
        sendClients[0].start();
        sendClients[0].send(new ProducerInitPacket(ClientType.PRODUCER_MAIN, CHANNEL_BASE), sendClientFuture);

        Thread.sleep(1000);

        String baseMessage = "testing";
        CompletableFuture<Void> sendMessageFuture = new CompletableFuture();
        String message = baseMessage;
        sendClients[0].send(new MessagePacket(message), sendMessageFuture);

        HashMap<String, Integer> consumerGroupNumMessages = new HashMap<>();
        for (String group : consumerGroups) {
            consumerGroupNumMessages.put(group, 0);
        }
        try {
            sendMessageFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
            for (int i = 0; i < receiveClients.length; i++) {
                try {
                    String receivedMessage = receiveMessageFutures[i].get(TIMEOUT, TimeUnit.MILLISECONDS);
                    Assert.assertEquals(baseMessage, receivedMessage);
                    consumerGroupNumMessages.put(consumerGroups[i], consumerGroupNumMessages.get(consumerGroups[i]) + 1);
                } catch (Exception e) {
                    // could be normal if a single consumer in the consumer group hasn't received message
                }
            }
        } catch (Exception e) {
            Assert.fail("Fail!", e);
        }

        for (String consumerGroup : consumerGroupNumMessages.keySet()) {
            Assert.assertTrue(1 == consumerGroupNumMessages.get(consumerGroup));
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

    @DataProvider(name = "consumerGroupsProvider")
    public Object[][] consumerGroupsProvider() {
        return new Object[][] {
                { new String[] {"A"} },
                { new String[] {"A", "B", "A" } },
                { new String[] {"A", "A"} }
        };
    }

    private void startWorkers() throws Exception {
        for (Worker worker : workers) {
            new Thread(() -> worker.start()).start();
        }
        Thread.sleep(1000);
    }
}
