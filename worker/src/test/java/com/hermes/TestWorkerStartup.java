package com.hermes;

import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

public class TestWorkerStartup {
    private static final String ZK_URL = "localhost:2181";
    private ZooKeeper zk;
    private Worker worker1;
    private Worker worker2;

    @BeforeClass
    public void setUp() {
        ZKManager.init(ZK_URL);
        zk = ZKManager.get();

        Initializer initializer = new Initializer(ZK_URL);
        initializer.run();
    }

    @Test
    public void testWorkerStartup() {
        worker1 = new Worker("A", "localhost", 3000);
        new Thread(() -> worker1.start()).start();

        worker2 = new Worker("localhost", 3001);
        new Thread(() -> worker2.start()).start();

        for (int i = 0; i < 3; i++) {
            // try 3 times to wait for worker creation in ZooKeeper
            try {
                List<String> workers = zk.getChildren(ZKPaths.WORKERS, null);
                if (workers.size() == 2) {
                    String w1 = workers.get(0);
                    String w2 = workers.get(1);
                    if ("A".equals(w1)) {
                        Assert.assertFalse("A".equals(w2));
                        Assert.assertEquals(new String(zk.getData(ZKPaths.WORKERS + "/A", null, null)), "localhost:3000");
                        Assert.assertEquals(new String(zk.getData(ZKPaths.WORKERS + "/" + w2, null, null)), "localhost:3001");
                    } else if ("A".equals(w2)) {
                        Assert.assertFalse("A".equals(w1));
                        Assert.assertEquals(new String(zk.getData(ZKPaths.WORKERS + "/" + w1, null, null)), "localhost:3001");
                        Assert.assertEquals(new String(zk.getData(ZKPaths.WORKERS + "/A", null, null)), "localhost:3000");
                    } else {
                        Assert.fail();
                    }
                    break;
                }
                Thread.sleep(1000);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        }
    }

    @AfterMethod
    public void afterMethod() {
        worker1.stop();
        worker2.stop();
    }

    @AfterClass
    public void tearDown() {
        ZKUtility.deleteChildren(ZKManager.get(), ZKPaths.ROOT, -1);
    }
}
