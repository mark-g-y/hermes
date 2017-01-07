package com.hermes;

import com.hermes.test.UsesZooKeeperTest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class TestSendMessage extends UsesZooKeeperTest {
    private static final String CHANNEL_BASE = "foobar";
    private static final String CONSUMER_GROUP = "A";
    private static final long TIMEOUT = 3000;

    private Worker[] workers;
    private Producer[] producers;
    private Consumer[] consumers;

    @Test(dataProvider = "clientServerCombos")
    public void testClientsServersReceiveAndDeliverMessage(int numProducers, int numWorkers, int numConsumers)
            throws Exception {
        workers = new Worker[numWorkers];
        com.hermes.worker.metadata.Worker[] workerData = new com.hermes.worker.metadata.Worker[numWorkers];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(Integer.toString(i), "localhost", 3000 + i);
            workerData[i] = new com.hermes.worker.metadata.Worker(Integer.toString(i), "localhost:" + (3000 + i), 0);
        }
        startWorkers();

        consumers = new Consumer[numConsumers];
        CompletableFuture<String>[] receiveMessageFutures = new CompletableFuture[consumers.length];
        for (int i = 0; i < consumers.length; i++) {
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
            consumers[i] = new Consumer(CHANNEL_BASE + i, CONSUMER_GROUP, receiver);
            consumers[i].start();
        }

        producers = new Producer[numProducers];
        for (int i = 0; i < producers.length; i++) {
            producers[i] = new Producer(CHANNEL_BASE + i);
            producers[i].start();
        }

        String baseMessage = "testing";
        for (int i = 0; i < producers.length; i++) {
            String message = baseMessage + i;
            producers[i].send(message, new ProducerCallback() {
                @Override
                public void onSuccess() {
                }
                @Override
                public void onFailure(Throwable th) {
                    Assert.fail();
                }
            });
        }

        try {
            for (int i = 0; i < consumers.length; i++) {
                String receivedMessage = receiveMessageFutures[i].get(TIMEOUT, TimeUnit.MILLISECONDS);
                Assert.assertEquals(baseMessage + i, receivedMessage);
            }
        } catch (Exception e) {
            Assert.fail("Fail!", e);
        }

        Thread.sleep(1000);
    }

    @AfterMethod
    public void afterMethod() {
        for (Worker worker : workers) {
            worker.stop();
        }
        for (Producer producer : producers) {
            producer.stop();
        }
        for (Consumer consumer : consumers) {
            consumer.stop();
        }
    }

    @DataProvider(name = "clientServerCombos")
    public Object[][] clientServerCombosProvider() {
        return new Object[][] {
                { 1, 1, 1 },
                { 3, 2, 2 },
                { 2, 1, 2}
        };
    }

    private void startWorkers() throws Exception {
        for (Worker worker : workers) {
            new Thread(() -> worker.start()).start();
        }
        Thread.sleep(1000);
    }
}
