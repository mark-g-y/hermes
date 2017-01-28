package com.hermes.test;

import com.hermes.Initializer;
import com.hermes.zookeeper.ZKManager;
import com.hermes.zookeeper.ZKPaths;
import com.hermes.zookeeper.ZKUtility;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/*
 * This helper class assumes ZooKeeper is already running in the test environment.
 */
public abstract class UsesZooKeeperTest {
    protected static final String ZK_URL = "localhost:2181";

    protected ZooKeeper zk;

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

    @AfterClass
    public void tearDown() {
        ZKUtility.deleteChildren(zk, ZKPaths.ROOT, -1);
    }
}
