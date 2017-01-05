package com.hermes;

import com.hermes.network.timeout.TimeoutConfig;
import com.hermes.partition.Partition;
import com.hermes.test.UsesZooKeeperTest;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TestSendMessageWithWorkerBackups extends UsesZooKeeperTest {
    private static final String CHANNEL = "foobar";
    private static final String PARTITION = Partition.get(CHANNEL);

    private Worker[] workers;
    private Producer producer;
    private Consumer consumer;

    @Test
    public void testSuccessWithBackups() throws Exception {
        workers = new Worker[3];
        com.hermes.worker.metadata.Worker[] workerData = new com.hermes.worker.metadata.Worker[workers.length];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(Integer.toString(i), "localhost", 3000 + i);
            workerData[i] = new com.hermes.worker.metadata.Worker(Integer.toString(i), "localhost:" + (3000 + i), 0);
            ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + i, null,
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        startWorkers();

        producer = new Producer(CHANNEL, 2);
        producer.start();

        String sentMessage = "testing";
        AtomicInteger numMessageReceptions = new AtomicInteger(0);
        consumer = new Consumer(CHANNEL, new Receiver() {
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
        consumer.start();

        producer.send(sentMessage, new ProducerCallback() {
            @Override
            public void onSuccess() {
            }
            @Override
            public void onFailure(Throwable th) {
            }
        });

        Thread.sleep((long)(TimeoutConfig.TIMEOUT * 3.5));

        Assert.assertEquals(1, numMessageReceptions.get());
    }

    @Test
    public void testWorkerFailure() throws Exception {
        workers = new Worker[4];
        com.hermes.worker.metadata.Worker[] workerData = new com.hermes.worker.metadata.Worker[workers.length];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(Integer.toString(i), "localhost", 3000 + i);
            workerData[i] = new com.hermes.worker.metadata.Worker(Integer.toString(i), "localhost:" + (3000 + i), 0);
            ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + i, null,
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        startWorkers();

        producer = new Producer(CHANNEL, 2);
        producer.start();

        String sentMessage = "testing";
        AtomicInteger numMessageReceptions = new AtomicInteger(0);
        consumer = new Consumer(CHANNEL, new Receiver() {
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
        consumer.start();

        // worker failure
        zk.delete(ZKPaths.PARTITIONS + "/" + PARTITION + "/" + 0, -1);
        zk.delete(ZKPaths.WORKERS + "/0", -1);
        workers[0].stop();
        Thread.sleep(1000);

        producer.send(sentMessage, new ProducerCallback() {
            @Override
            public void onSuccess() {
            }
            @Override
            public void onFailure(Throwable th) {
            }
        });

        Thread.sleep((long)(TimeoutConfig.TIMEOUT * 3.5));

        Assert.assertEquals(1, numMessageReceptions.get());
    }

    @AfterMethod
    public void afterMethod() {
        for (Worker worker : workers) {
            worker.stop();
        }
        producer.stop();
        consumer.stop();
    }

    private void startWorkers() throws Exception {
        for (Worker worker : workers) {
            new Thread(() -> worker.start()).start();
        }
        Thread.sleep(1000);
    }
}
