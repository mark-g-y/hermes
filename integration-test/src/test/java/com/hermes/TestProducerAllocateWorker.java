package com.hermes;

import com.hermes.partition.Partition;
import com.hermes.test.UsesZooKeeperTest;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class TestProducerAllocateWorker extends UsesZooKeeperTest {
    private static final String CHANNEL = "foobar";
    private static final String PARTITION = Partition.get(CHANNEL);

    private Worker[] workers;

    @Test
    public void testAllocateWorkersToPartition() throws Exception {
        workers = new Worker[] {new Worker("A", "localhost", 3000), new Worker("B", "localhost", 3001),
                                new Worker("C", "localhost", 3002)};
        startWorkers();
        Thread.sleep(1000);

        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/A", null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Producer producer = new Producer(ZK_URL, CHANNEL, 2);
        producer.start();
        Thread.sleep(1000);

        List<String> assignedWorkers = zk.getChildren(ZKPaths.PARTITIONS + "/" + PARTITION, null);
        Assert.assertEquals(assignedWorkers.size(), 3);
        Assert.assertTrue(assignedWorkers.containsAll(Arrays.asList(new String[]{"A", "B", "C"})));

        producer.stop();
    }

    @Test
    public void testAllocateDelayedStartWorkerToPartition() throws Exception {
        workers = new Worker[] {new Worker("A", "localhost", 3000), new Worker("B", "localhost", 3001),
                                new Worker("C", "localhost", 3002)};
        for (int i = 1; i < workers.length; i++) {
            final int index = i;
            new Thread(() -> workers[index].start()).start();
        }
        Thread.sleep(1000);

        ZKUtility.createIgnoreExists(zk, ZKPaths.PARTITIONS + "/" + PARTITION + "/B", null,
                                     ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Producer producer = new Producer(ZK_URL, CHANNEL, 2);
        new Thread(() -> producer.start()).start();
        Thread.sleep(1000);
        new Thread(() -> workers[0].start()).start();
        Thread.sleep(1000);

        List<String> assignedWorkers = zk.getChildren(ZKPaths.PARTITIONS + "/" + PARTITION, null);
        Assert.assertEquals(assignedWorkers.size(), 3);
        Assert.assertTrue(assignedWorkers.containsAll(Arrays.asList(new String[]{"A", "B", "C"})));

        producer.stop();
    }

    @AfterMethod
    public void afterMethod() {
        for (Worker worker : workers) {
            worker.stop();
        }
    }

    private void startWorkers() {
        for (Worker worker : workers) {
            new Thread(() -> worker.start()).start();
        }
    }
}
