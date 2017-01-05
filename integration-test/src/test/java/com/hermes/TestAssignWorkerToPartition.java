package com.hermes;

import com.hermes.partition.Partition;
import com.hermes.worker.WorkerManager;
import com.hermes.test.UsesZooKeeperTest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class TestAssignWorkerToPartition extends UsesZooKeeperTest {
    private Worker[] workers;

    @Test
    public void testAssignWorkerToFirstPartition() throws Exception {
        workers = new Worker[] { new Worker("localhost", 3000) };
        for (Worker worker : workers) {
            new Thread(() -> worker.start()).start();
        }

        String url = WorkerManager.selectWorkersForPartition(Partition.get("foobar"), 1).get(0).getUrl();

        Assert.assertEquals("localhost:3000", url);
    }

    @Test
    public void testAssignWorkerToSecondPartition() throws Exception {
        workers = new Worker[] { new Worker("localhost", 3001)};
        for (Worker worker : workers) {
            new Thread(() -> worker.start()).start();
        }

        String url = WorkerManager.selectWorkersForPartition(Partition.get("foobar"), 1).get(0).getUrl();
        String url2 = WorkerManager.selectWorkersForPartition(Partition.get("foobar2"), 1).get(0).getUrl();

        Assert.assertEquals("localhost:3001", url);
        Assert.assertEquals("localhost:3001", url2);
    }

    @AfterMethod
    public void afterMethod() {
        for (Worker worker : workers) {
            worker.stop();
        }
    }
}
