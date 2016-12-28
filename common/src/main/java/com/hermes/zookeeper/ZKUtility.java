package com.hermes.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.util.List;

public class ZKUtility {
    public static boolean createIgnoreExists(ZooKeeper zk, String path, byte[] data, List<ACL> acl,
                                             CreateMode createMode)
            throws KeeperException, InterruptedException {
        try {
            zk.create(path, data, acl, createMode);
            return true;
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
            return false;
        } catch (InterruptedException e) {
            throw e;
        }
    }

    public static void deleteChildren(ZooKeeper zk, String path, int version) {
        try {
            List<String> children = zk.getChildren(path, null);
            for (String child : children) {
                deleteChildren(zk, path + "/" + child, version);
                zk.delete(path + "/" + child, version);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
