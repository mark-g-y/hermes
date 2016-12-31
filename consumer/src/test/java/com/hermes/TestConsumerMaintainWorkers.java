package com.hermes;

import com.hermes.partition.Partition;
import com.hermes.worker.ConnectionCounterWorker;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import junit.framework.Assert;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.*;

public class TestConsumerMaintainWorkers {
    private static final String ZK_URL = "localhost:2181";
    private static final String CHANNEL = "foobar";
    private static final String PARTITION = Partition.get(CHANNEL);

    private ZooKeeper zk;
    private ConnectionCounterWorker[] workers;

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
    }

    @Test
    public void testConnectExistingWorkers() throws Exception {
        workers = new ConnectionCounterWorker[] {new ConnectionCounterWorker("1", 3000),
                                                 new ConnectionCounterWorker("2", 3001),
                                                 new ConnectionCounterWorker("3", 3002)};
        startWorkers();

        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + workers[0].getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + workers[1].getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Consumer consumer = new Consumer(CHANNEL, new Receiver() {
            @Override
            public void onMessageReceived(String message) {
            }
            @Override
            public void onDisconnect(Throwable throwable) {
            }
        });
        consumer.start();
        consumer.stop();

        Assert.assertEquals(workers[0].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(workers[1].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(workers[2].getNumPreviousAndCurrentConnections(), 0);
    }

    @Test
    public void testDetectAndConnectNewWorker() throws Exception {
        workers = new ConnectionCounterWorker[] {new ConnectionCounterWorker("1", 3000),
                                                 new ConnectionCounterWorker("2", 3001)};
        startWorkers();

        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + workers[0].getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + workers[1].getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Consumer consumer = new Consumer(CHANNEL, new Receiver() {
            @Override
            public void onMessageReceived(String message) {
            }
            @Override
            public void onDisconnect(Throwable throwable) {
            }
        });
        consumer.start();

        ConnectionCounterWorker newWorker = new ConnectionCounterWorker("3", 3002);
        new Thread(() -> newWorker.start()).start();

        ConnectionCounterWorker newWorker2 = new ConnectionCounterWorker("4", 3003);
        new Thread(() -> newWorker2.start()).start();

        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + newWorker.getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + newWorker2.getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // wait for ZooKeeper watch to trigger and consumer to connect to new client
        Thread.sleep(7500);

        newWorker.stop();
        newWorker2.stop();
        consumer.stop();

        Assert.assertEquals(workers[0].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(workers[1].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(newWorker.getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(newWorker2.getNumPreviousAndCurrentConnections(), 1);
    }

    @Test
    public void testDetectAndDisconnectDroppedWorker() throws Exception {
        workers = new ConnectionCounterWorker[] {new ConnectionCounterWorker("1", 3000),
                                                 new ConnectionCounterWorker("2", 3001),
                                                 new ConnectionCounterWorker("3", 3002)};
        startWorkers();

        for (ConnectionCounterWorker worker : workers) {
            ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + worker.getId(), null,
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        Consumer consumer = new Consumer(CHANNEL, new Receiver() {
            @Override
            public void onMessageReceived(String message) {
            }
            @Override
            public void onDisconnect(Throwable throwable) {
            }
        });
        consumer.start();

        // simulate worker failure
        workers[2].stop();
        zk.delete(ZKPaths.PARTITIONS + "/" + PARTITION + "/" + workers[2].getId(), -1);

        // wait for worker failure to be detected
        Thread.sleep(5000);

        // despite worker failing, consumer should not try to reconnect to non-failing workers
        // hence, the number of previous & current connections to each should still be 1
        Assert.assertEquals(workers[0].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(workers[1].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(workers[2].getNumPreviousAndCurrentConnections(), 1);
    }

    @AfterMethod
    public void afterMethod() {
        for (ConnectionCounterWorker worker : workers) {
            worker.stop();
        }
    }

    @AfterClass
    public void tearDown() {
        ZKUtility.deleteChildren(zk, ZKPaths.ROOT, -1);
    }

    private void startWorkers() {
        for (ConnectionCounterWorker worker : workers) {
            new Thread(() -> worker.start()).start();
        }
    }
}
