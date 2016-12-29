package com.hermes;

import com.hermes.partition.Partition;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import junit.framework.Assert;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.*;

import java.util.concurrent.CompletableFuture;

public class TestProducerConnection {
    private static final String ZK_URL = "localhost:2181";
    private static final String CHANNEL = "foobar";
    private static final String PARTITION = Partition.get(CHANNEL);

    private ZooKeeper zk;
    private MockWorker mockWorker1;
    private MockWorker mockWorker2;

    @BeforeClass
    public void setUp() {
        ZKManager.init(ZK_URL);
        zk = ZKManager.get();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        ZKUtility.deleteChildren(zk, ZKPaths.ROOT, -1);
        Initializer initializer = new Initializer(ZK_URL);
        initializer.run();

        mockWorker1 = new MockWorker(3000);
        new Thread(() -> mockWorker1.start()).start();
        mockWorker2 = new MockWorker(3001);
        new Thread(() -> mockWorker2.start()).start();

        ZKUtility.createIgnoreExists(zk, ZKPaths.WORKERS + "/" + "workerA", "localhost:3000".getBytes(),
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        ZKUtility.createIgnoreExists(zk, ZKPaths.WORKERS + "/" + "workerB", "localhost:3001".getBytes(),
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    @Test
    public void testProducerConnectToWorker() throws Exception {
        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/workerA", null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        CompletableFuture<Void> future = new CompletableFuture<>();
        ProducerCallback callback = new ProducerCallback() {
            @Override
            public void onSuccess() {
                future.complete(null);
            }
            @Override
            public void onFailure(Throwable th) {
                future.completeExceptionally(th);
            }
        };
        Producer producer = new Producer(CHANNEL);
        try {
            String message = "testing";
            producer.start();
            producer.send(message, callback);
            future.get();
            Assert.assertTrue(mockWorker1.getReceivedMessages().contains(message));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        producer.stop();
    }

    @Test
    public void testReconnectDifferentWorkerOnFailure() throws Exception {
        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/workerA", null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        CompletableFuture<Void> future = new CompletableFuture<>();
        ProducerCallback callback = new ProducerCallback() {
            @Override
            public void onSuccess() {
                future.complete(null);
            }
            @Override
            public void onFailure(Throwable th) {
                future.completeExceptionally(th);
            }
        };
        Producer producer = new Producer(CHANNEL);
        producer.start();

        // simulate new worker creation but old worker failure
        zk.delete(ZKPaths.PARTITIONS + "/" + PARTITION + "/workerA", -1);
        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/workerB", null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        mockWorker1.stop();

        try {
            String message = "testing";
            producer.send(message, callback);
            future.get();
            Assert.assertFalse(mockWorker1.getReceivedMessages().contains(message));
            Assert.assertTrue(mockWorker2.getReceivedMessages().contains(message));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        producer.stop();
    }

    @AfterMethod
    public void afterMethod() {
        mockWorker1.getReceivedMessages().clear();
        mockWorker1.stop();
        mockWorker2.getReceivedMessages().clear();
        mockWorker2.stop();
    }

    @AfterClass
    public void tearDown() {
        ZKUtility.deleteChildren(zk, ZKPaths.ROOT, -1);
    }
}
