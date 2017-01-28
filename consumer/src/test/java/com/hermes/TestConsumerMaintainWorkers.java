package com.hermes;

import com.hermes.partition.Partition;
import com.hermes.test.UsesZooKeeperTest;
import com.hermes.worker.ConnectionCounterSocketServer;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import junit.framework.Assert;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.testng.annotations.*;

public class TestConsumerMaintainWorkers extends UsesZooKeeperTest {
    private static final String CHANNEL = "foobar";
    private static final String CONSUMER_GROUP = "A";
    private static final String PARTITION = Partition.get(CHANNEL);

    private ConnectionCounterSocketServer[] servers;

    @Test
    public void testConnectExistingWorkers() throws Exception {
        servers = new ConnectionCounterSocketServer[] { new ConnectionCounterSocketServer("1", 3000),
                                                        new ConnectionCounterSocketServer("2", 3001),
                                                        new ConnectionCounterSocketServer("3", 3002) };
        startWorkers();

        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + servers[0].getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + servers[1].getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Consumer consumer = new Consumer(ZK_URL, CHANNEL, CONSUMER_GROUP, (message) -> {});
        consumer.start();
        Thread.sleep(1000);
        consumer.stop();

        Assert.assertEquals(servers[0].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(servers[1].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(servers[2].getNumPreviousAndCurrentConnections(), 0);
    }

    @Test
    public void testDetectAndConnectNewWorker() throws Exception {
        servers = new ConnectionCounterSocketServer[] { new ConnectionCounterSocketServer("1", 3000),
                                                        new ConnectionCounterSocketServer("2", 3001) };
        startWorkers();

        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + servers[0].getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + servers[1].getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Consumer consumer = new Consumer(ZK_URL, CHANNEL, CONSUMER_GROUP, (message) -> {});
        consumer.start();
        Thread.sleep(1000);

        ConnectionCounterSocketServer newServer = new ConnectionCounterSocketServer("3", 3002);
        new Thread(() -> newServer.start()).start();

        ConnectionCounterSocketServer newServer2 = new ConnectionCounterSocketServer("4", 3003);
        new Thread(() -> newServer2.start()).start();

        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + newServer.getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + newServer2.getId(), null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // wait for ZooKeeper watch to trigger and consumer to connect to new client
        Thread.sleep(7500);

        newServer.stop();
        newServer2.stop();
        consumer.stop();

        Assert.assertEquals(servers[0].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(servers[1].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(newServer.getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(newServer2.getNumPreviousAndCurrentConnections(), 1);
    }

    @Test
    public void testDetectAndDisconnectDroppedWorker() throws Exception {
        servers = new ConnectionCounterSocketServer[] {new ConnectionCounterSocketServer("1", 3000),
                                                       new ConnectionCounterSocketServer("2", 3001),
                                                       new ConnectionCounterSocketServer("3", 3002)};
        startWorkers();

        for (ConnectionCounterSocketServer server : servers) {
            ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/" + server.getId(), null,
                                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        Consumer consumer = new Consumer(ZK_URL, CHANNEL, CONSUMER_GROUP, (message) -> {});
        consumer.start();

        // simulate worker failure
        servers[2].stop();
        zk.delete(ZKPaths.PARTITIONS + "/" + PARTITION + "/" + servers[2].getId(), -1);

        // wait for worker failure to be detected
        Thread.sleep(5000);

        // despite worker failing, consumer should not try to reconnect to non-failing workers
        // hence, the number of previous & current connection to each should still be 1
        Assert.assertEquals(servers[0].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(servers[1].getNumPreviousAndCurrentConnections(), 1);
        Assert.assertEquals(servers[2].getNumPreviousAndCurrentConnections(), 1);
    }

    @AfterMethod
    public void afterMethod() {
        for (ConnectionCounterSocketServer server : servers) {
            server.stop();
        }
    }

    private void startWorkers() {
        for (ConnectionCounterSocketServer server : servers) {
            new Thread(() -> server.start()).start();
        }
    }
}
