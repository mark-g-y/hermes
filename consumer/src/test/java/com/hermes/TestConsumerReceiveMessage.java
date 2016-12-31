package com.hermes;

import com.hermes.partition.Partition;
import com.hermes.test.UsesZooKeeperTest;
import com.hermes.worker.AbstractMockWorker;
import com.hermes.worker.MessageBroadcastWorker;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestConsumerReceiveMessage extends UsesZooKeeperTest {
    private static final String CHANNEL = "foobar";
    private static final String PARTITION = Partition.get(CHANNEL);
    private static final int TIMEOUT = 3000;

    private Consumer consumer;
    private MessageBroadcastWorker[] workers;

    @Test
    public void testConsumerReceiveMessage() throws Exception {
        workers = new MessageBroadcastWorker[1];
        workers[0] = new MessageBroadcastWorker("broadcaster", 3000);
        startWorkers();

        CompletableFuture<String> receiveMessageFuture = new CompletableFuture<>();
        consumer = new Consumer(CHANNEL, new Receiver() {
            @Override
            public void onMessageReceived(String message) {
                receiveMessageFuture.complete(message);
            }
            @Override
            public void onDisconnect(Throwable throwable) {
                receiveMessageFuture.completeExceptionally(throwable);
            }
        });
        consumer.start();

        String message = "Test";
        List<CompletableFuture<Void>> ackFutures = workers[0].broadcastMessage(message);

        try {
            String receivedMessage = receiveMessageFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
            Assert.assertEquals(message, receivedMessage);
            CompletableFuture.allOf((CompletableFuture[])ackFutures.toArray(new CompletableFuture[0]))
                    .get(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            Assert.fail("Failed!", e);
        }
    }

    @Test
    public void testConsumerReceiveMessagesFromMultipleWorkers() throws Exception {
        workers = new MessageBroadcastWorker[2];
        workers[0] = new MessageBroadcastWorker("broadcaster1", 3000);
        workers[1] = new MessageBroadcastWorker("broadcaster2", 3001);
        startWorkers();

        AtomicInteger completedIndex = new AtomicInteger(0);
        CompletableFuture<String>[] receiveMessageFutures = new CompletableFuture[workers.length];
        for (int i = 0;i < receiveMessageFutures.length; i++) {
            receiveMessageFutures[i] = new CompletableFuture<>();
        }
        consumer = new Consumer(CHANNEL, new Receiver() {
            @Override
            public void onMessageReceived(String message) {
                receiveMessageFutures[completedIndex.getAndIncrement()].complete(message);
            }
            @Override
            public void onDisconnect(Throwable throwable) {
                receiveMessageFutures[completedIndex.getAndIncrement()].completeExceptionally(throwable);
            }
        });
        consumer.start();

        String[] messages = new String[] {"test1", "test2"};
        List<CompletableFuture<Void>>[] ackFutures = new List[workers.length];
        for (int i = 0; i < workers.length; i++) {
            ackFutures[i] = workers[i].broadcastMessage(messages[i]);
        }

        String[] receivedMessages = new String[messages.length];
        try {
            for (int i = 0; i < receivedMessages.length; i++) {
                receivedMessages[i] = receiveMessageFutures[i].get(TIMEOUT, TimeUnit.MILLISECONDS);
            }
            List<String> receivedMessagesList = Arrays.asList(receivedMessages);
            for (int i = 0; i < messages.length; i++) {
                Assert.assertTrue(receivedMessagesList.contains(messages[i]));
            }

            for (int i = 0; i < ackFutures.length; i++) {
                CompletableFuture.allOf((CompletableFuture[]) ackFutures[i].toArray(new CompletableFuture[0]))
                        .get(TIMEOUT, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            Assert.fail("Failed!", e);
        }
    }

    @AfterMethod
    public void afterMethod() {
        consumer.stop();
        for (AbstractMockWorker worker : workers) {
            worker.stop();
        }
    }

    private void startWorkers() throws Exception {
        for (AbstractMockWorker worker : workers) {
            new Thread(() -> worker.start()).start();
            ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + worker.getId(), null,
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
}
