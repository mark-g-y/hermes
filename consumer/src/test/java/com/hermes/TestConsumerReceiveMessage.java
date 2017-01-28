package com.hermes;

import com.hermes.network.SocketServer;
import com.hermes.partition.Partition;
import com.hermes.test.UsesZooKeeperTest;
import com.hermes.worker.MessageBroadcastSocketServer;
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
    private static final String CONSUMER_GROUP = "A";
    private static final String PARTITION = Partition.get(CHANNEL);
    private static final int TIMEOUT = 3000;

    private Consumer consumer;
    private MessageBroadcastSocketServer[] servers;

    @Test
    public void testConsumerReceiveMessage() throws Exception {
        servers = new MessageBroadcastSocketServer[1];
        servers[0] = new MessageBroadcastSocketServer("broadcaster", 3000);
        startServers();

        CompletableFuture<String> receiveMessageFuture = new CompletableFuture<>();
        consumer = new Consumer(ZK_URL, CHANNEL, CONSUMER_GROUP, (message) -> receiveMessageFuture.complete(message));
        consumer.start();
        Thread.sleep(1000);

        String message = "Test";
        List<CompletableFuture<Void>> ackFutures = servers[0].broadcastMessage(message);

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
        servers = new MessageBroadcastSocketServer[2];
        servers[0] = new MessageBroadcastSocketServer("broadcaster1", 3000);
        servers[1] = new MessageBroadcastSocketServer("broadcaster2", 3001);
        startServers();

        AtomicInteger completedIndex = new AtomicInteger(0);
        CompletableFuture<String>[] receiveMessageFutures = new CompletableFuture[servers.length];
        for (int i = 0;i < receiveMessageFutures.length; i++) {
            receiveMessageFutures[i] = new CompletableFuture<>();
        }
        consumer = new Consumer(ZK_URL, CHANNEL, CONSUMER_GROUP, (message) ->
                receiveMessageFutures[completedIndex.getAndIncrement()].complete(message));
        consumer.start();
        Thread.sleep(1000);

        String[] messages = new String[] {"test1", "test2"};
        List<CompletableFuture<Void>>[] ackFutures = new List[servers.length];
        for (int i = 0; i < servers.length; i++) {
            ackFutures[i] = servers[i].broadcastMessage(messages[i]);
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
        for (SocketServer server : servers) {
            server.stop();
        }
    }

    private void startServers() throws Exception {
        for (MessageBroadcastSocketServer server : servers) {
            new Thread(() -> server.start()).start();
            ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + server.getId(),
                                         null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
}
