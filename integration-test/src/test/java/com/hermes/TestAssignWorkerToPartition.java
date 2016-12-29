package com.hermes;

import com.hermes.client.workerallocation.WorkerManager;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestAssignWorkerToPartition {
    private static final String ZK_URL = "localhost:2181";
    private ZooKeeper zk;
    private Worker[] workers;

    @BeforeClass
    public void setUp() {
        ZKManager.init(ZK_URL);
        zk = ZKManager.get();
    }

    @BeforeMethod
    public void beforeMethod() {
        ZKUtility.deleteChildren(zk, ZKPaths.ROOT, -1);
        Initializer initializer = new Initializer(ZK_URL);
        initializer.run();
    }

    @Test
    public void testAssignWorkerToFirstPartition() throws Exception {
        workers = new Worker[] { new Worker("localhost", 3000) };
        for (Worker worker : workers) {
            new Thread(() -> worker.start()).start();
        }

        String url = WorkerManager.selectWorkers(WorkerManager.getAllWorkersForChannel("foobar"), 1).get(0).getUrl();

        Assert.assertEquals("localhost:3000", url);
    }

    @Test
    public void testAssignWorkerToSecondPartition() throws Exception {
        workers = new Worker[] { new Worker("localhost", 3001)};
        for (Worker worker : workers) {
            new Thread(() -> worker.start()).start();
        }

        String url = WorkerManager.selectWorkers(WorkerManager.getAllWorkersForChannel("foobar"), 1).get(0).getUrl();
        String url2 = WorkerManager.selectWorkers(WorkerManager.getAllWorkersForChannel("foobar2"), 1).get(0).getUrl();

        Assert.assertEquals("localhost:3001", url);
        Assert.assertEquals("localhost:3001", url2);
    }

    @AfterMethod
    public void afterMethod() {
        for (Worker worker : workers) {
            worker.stop();
        }
        ZKUtility.deleteChildren(zk, ZKPaths.ROOT, -1);
    }
}
